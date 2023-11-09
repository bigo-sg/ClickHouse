#include <Interpreters/Set.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/Context.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFunction.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <optional>
#include <Columns/ColumnSet.h>
#include <queue>
#include <shared_mutex>
#include <stack>
#include <base/sort.h>
#include <Common/JSONBuilder.h>
#include <Core/SettingsEnums.h>

#include <Columns/MaskOperations.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>
#include <algorithm>
#include <cmath>
#include <unordered_map>
#include <unordered_set>


#if defined(MEMORY_SANITIZER)
    #include <sanitizer/msan_interface.h>
#endif

#if defined(ADDRESS_SANITIZER)
    #include <sanitizer/asan_interface.h>
#endif

namespace ProfileEvents
{
    extern const Event FunctionExecute;
    extern const Event CompiledFunctionExecute;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int TOO_MANY_TEMPORARY_COLUMNS;
    extern const int TOO_MANY_TEMPORARY_NON_CONST_COLUMNS;
    extern const int TYPE_MISMATCH;
}

ExpressionShortCircuitExecuteController::ExpressionShortCircuitExecuteController(
    const ActionsDAGPtr & actions_dag_,
    const ExpressionActionsSettings & settings)
    : actions_dag(actions_dag_)
    , short_circuit_function_evaluation(settings.short_circuit_function_evaluation)
    , enable_adaptive_reorder_arguments(settings.enable_adaptive_reorder_short_circuit_arguments)
    , max_sample_rows(settings.adaptive_reorder_short_circuit_arguments_sample_rows)
    , max_allowed_arguments(settings.max_arguments_for_adaptive_reorder_short_circuit_arguments)
{
    const auto & nodes = actions_dag->getNodes();
    for (const auto & node : nodes)
    {
        /// For default, don't change the arguments' order.
        ShortCircuitInfo info;
        for (size_t i = 0; i < node.children.size(); ++i)
            info.arguments_position.emplace_back(i);
        short_circuit_infos[&node] = info;
    }
    auto lazy_executed_nodes = processShortCircuitFunctions();

#if USE_EMBEDDED_COMPILER
    /// Since we will just reorder the arguments position, this will not change the behavior of compiling functions
    if (settings.can_compile_expressions && settings.compile_expressions == CompileExpressions::yes)
        actions_dag->compileExpressions(settings.min_count_to_compile_expression, lazy_executed_nodes);

    /// New children may be added into the compiled functions. refresh all arguments_position.
    const auto & compiled_nodes = actions_dag->getNodes();
    for (const auto & node : compiled_nodes)
    {
        auto & info = short_circuit_infos[&node];
        info.arguments_position.clear();
        for (size_t i = 0; i < node.children.size(); ++i)
            info.arguments_position.emplace_back(i);
    }
#endif

    bool has_too_many_arguments = false;
    for (const auto & node : nodes)
    {
        if (node.type == ActionsDAG::ActionType::FUNCTION)
        {
            IFunctionBase::ShortCircuitSettings short_circuit_settings;
            if (node.function_base->isShortCircuit(short_circuit_settings, node.children.size()))
            {
                if (short_circuit_settings.support_reorder_arguments)
                    has_reorderable_short_circuit_functions = true;
                for (const auto * child : node.children)
                    short_circuit_infos[child].is_short_circuit_function_child = true;
                /// If the arguments size is too large, cost of the sample process will
                /// be every large.
                if (node.children.size() > max_allowed_arguments)
                   has_too_many_arguments = true;
            }
        }
    }

    if (has_too_many_arguments || !has_reorderable_short_circuit_functions || !enable_adaptive_reorder_arguments
        || short_circuit_function_evaluation == ShortCircuitFunctionEvaluation::DISABLE)
    {
        finished_adaptive_reorder_arguements = true;

        if (short_circuit_function_evaluation != ShortCircuitFunctionEvaluation::DISABLE)
            markLazyExecutedNodes(lazy_executed_nodes);
    }
}

bool ExpressionShortCircuitExecuteController::couldLazyExecuted(const ActionsDAG::Node * node)
{
    if (enable_adaptive_reorder_arguments && !finished_adaptive_reorder_arguements)
        return false;
    return short_circuit_infos[node].is_lazy_executed;
}

int ExpressionShortCircuitExecuteController::needProfile(const ActionsDAG::Node * node)
{
    int res = NOT_PROFILE;
    if (!enable_adaptive_reorder_arguments || finished_adaptive_reorder_arguements)
        return res;
    res |= PROFILE_ELAPSED;
    if (short_circuit_infos[node].is_short_circuit_function_child && isNativeNumber(*node->result_type))
        res |= PROFILE_SELECTIVITY;
    return res;
}

void ExpressionShortCircuitExecuteController::addNodeShortCircuitProfile(const ActionsDAG::Node * node, const ProfileData & profile_data_)
{
    // LOG_ERROR(&Poco::Logger::get("ExpressionShortCircuitExecuteController"), "addNodeShortCircuitProfile:{}, sample_rows: {}, selected_rows: {}", node->result_name, profile_data_.sample_rows, profile_data_.selected_rows);
    auto & info = short_circuit_infos[node];
    std::unique_lock lock(mutex);
    info.profile_data.sample_rows += profile_data_.sample_rows;
    info.profile_data.selected_rows += profile_data_.selected_rows;
    info.profile_data.elapsed += profile_data_.elapsed;
}

void ExpressionShortCircuitExecuteController::tryReorderShortCircuitFunctionsAguments(size_t num_rows)
{
    if (finished_adaptive_reorder_arguements.load() || !num_rows || sampled_rows.load() >= max_sample_rows)
        return;
    sampled_rows += num_rows;
    if (sampled_rows >= max_sample_rows)
    {
        std::unique_lock lock(mutex);
        if (finished_adaptive_reorder_arguements)
            return;
        reorderShortCircuitFunctionsAguments();
        auto lazy_executed_nodes = processShortCircuitFunctions();
        markLazyExecutedNodes(lazy_executed_nodes);
        finished_adaptive_reorder_arguements = true;
    }
}

std::vector<size_t> ExpressionShortCircuitExecuteController::getReorderedArgumentsPosition(const ActionsDAG::Node * node)
{
    if (finished_adaptive_reorder_arguements)
        return short_circuit_infos[node].arguments_position;
    std::shared_lock lock(mutex);
    return short_circuit_infos[node].arguments_position;
}

size_t ExpressionShortCircuitExecuteController::needSampleRows() const
{
    if (!enable_adaptive_reorder_arguments || finished_adaptive_reorder_arguements)
        return 0;
    size_t n = sampled_rows.load();
    return max_sample_rows > n ? max_sample_rows - n : 0;
}

double ExpressionShortCircuitExecuteController::calculateNodeElapsed(const ActionsDAG::Node * node)
{
    const auto & info = short_circuit_infos[node];
    double res = info.profile_data.sample_rows ? info.profile_data.elapsed * 1.0 / info.profile_data.sample_rows : 0.0;
    for (const auto * child : node->children)
    {
        res += calculateNodeElapsed(child);
    }
    return res;
}

void ExpressionShortCircuitExecuteController::reorderShortCircuitFunctionsAguments()
{
    for (auto & [node, info] : short_circuit_infos)
    {
        if (node->type != ActionsDAG::ActionType::FUNCTION && node->children.size() < 2)
            continue;
        /// If the function is compiled, the arguments are different from the original one.
        /// At presesnt, is_function_compiled = false, when this function has lazy executed child.
        if (node->is_function_compiled)
            continue;
        IFunctionBase::ShortCircuitSettings short_circuit_settings;
        if (!node->function_base->isShortCircuit(short_circuit_settings, node->children.size()))
            continue;
        if (!short_circuit_settings.support_reorder_arguments)
            continue;

        /// FIXME. A special case, select and(a,b,a) from t. if we change `and(a,b,a)` to
        /// and(b,a,a), `a` will be executed twice. This is not a stable case, so skip it.
        std::unordered_set<const ActionsDAG::Node *> unique_children;
        for (const auto * child : node->children)
        {
            unique_children.insert(child);
        }
        if (unique_children.size() != node->children.size())
            continue;

        std::unordered_map<const ActionsDAG::Node *, double> node_elapsed;
        for (const auto * child : node->children)
        {
            auto elpased = calculateNodeElapsed(child);
            node_elapsed[child] = elpased;
        }

        std::vector<std::pair<UInt64, double>> args_ranks;
        for (size_t i = 0; i < node->children.size(); ++i)
        {
            const auto & child_info = short_circuit_infos[node->children[i]];
            auto child_elapsed = node_elapsed[node->children[i]];
            double selectivity = child_info.profile_data.selected_rows * 1.0 / child_info.profile_data.sample_rows;
            selectivity = selectivity < 0.0000001 ? 0.0000001 : selectivity; // in case it's zero.
            if (short_circuit_settings.is_inverted_select)
                selectivity = 1.0 / selectivity;
            double cost = child_elapsed * selectivity;
            args_ranks.push_back({i, cost});
            LOG_TRACE(
                &Poco::Logger::get("ExpressionShortCircuitExecuteController"),
                "calculate execute cost. node: {}, argument position:{}, selectivity:{}(by {}/{}), cost:{}(by {}), rank value:{}",
                node->result_name,
                i,
                selectivity,
                child_info.profile_data.selected_rows,
                child_info.profile_data.sample_rows,
                cost,
                child_elapsed,
                args_ranks.back().second);
        }
        std::sort(
            args_ranks.begin(),
            args_ranks.end(),
            [](const std::pair<UInt64, double> & a, const std::pair<UInt64, double> & b) { return a.second < b.second; });
        for (size_t i = 0; i < node->children.size(); ++i)
        {
            LOG_TRACE(
                &Poco::Logger::get("ExpressionShortCircuitExecuteController"),
                "move the {} argument of {} to position {} on rank value {}",
                args_ranks[i].first,
                args_ranks[i].second,
                node->result_name,
                i);
            info.arguments_position[i] = args_ranks[i].first;
        }
    }
}

void ExpressionShortCircuitExecuteController::disableAdaptiveReorderArguments()
{
    std::unique_lock lock(mutex);
    if (!enable_adaptive_reorder_arguments)
        return;

    enable_adaptive_reorder_arguments = false;
    for (auto & [node, info] : short_circuit_infos)
    {
        for (size_t i = 0, n = info.arguments_position.size(); i < n; ++i)
        {
            info.arguments_position[i] = i;
        }
    }
    auto lazy_executed_nodes = processShortCircuitFunctions();
    markLazyExecutedNodes(lazy_executed_nodes);
    finished_adaptive_reorder_arguements = true;
}


static ActionsDAGReverseInfo getActionsDAGReverseInfo(const std::list<ActionsDAG::Node> & nodes, const ActionsDAG::NodeRawConstPtrs & index);
std::unordered_set<const ActionsDAG::Node *>  ExpressionShortCircuitExecuteController::processShortCircuitFunctions()
{
    const auto & nodes = actions_dag->getNodes();
    /// Firstly, find all short-circuit functions and get their settings.
    std::unordered_map<const ActionsDAG::Node *, IFunctionBase::ShortCircuitSettings> short_circuit_nodes;
    IFunctionBase::ShortCircuitSettings short_circuit_settings;
    for (const auto & node : nodes)
    {
        if (node.type == ActionsDAG::ActionType::FUNCTION && node.function_base->isShortCircuit(short_circuit_settings, node.children.size()) && !node.children.empty())
            short_circuit_nodes[&node] = short_circuit_settings;
    }

    /// If there are no short-circuit functions, no need to do anything.
    if (short_circuit_nodes.empty())
        return {};

    auto reverse_info = getActionsDAGReverseInfo(nodes, actions_dag->getOutputs());

    /// For each node we fill LazyExecutionInfo.
    std::unordered_map<const ActionsDAG::Node *, LazyExecutionInfo> lazy_execution_infos;
    for (const auto & node : nodes)
        setLazyExecutionInfo(&node, reverse_info, short_circuit_nodes, lazy_execution_infos);

    std::unordered_set<const ActionsDAG::Node *> lazy_executed_nodes;
    for (const auto & [node, settings] : short_circuit_nodes)
    {
        /// Recursively find nodes that should be lazy executed.
        findLazyExecutedNodes(
            node->children,
            lazy_execution_infos,
            settings.force_enable_lazy_execution || short_circuit_function_evaluation == ShortCircuitFunctionEvaluation::FORCE_ENABLE,
            lazy_executed_nodes);
    }
    return lazy_executed_nodes;
}


void ExpressionShortCircuitExecuteController::setLazyExecutionInfo(
        const ActionsDAG::Node * node,
        const ActionsDAGReverseInfo & reverse_info,
        const std::unordered_map<const ActionsDAG::Node *, IFunctionBase::ShortCircuitSettings> & short_circuit_nodes,
        std::unordered_map<const ActionsDAG::Node *, LazyExecutionInfo> & lazy_execution_infos)
{
    /// If we already created info about this node, just do nothing.
    if (lazy_execution_infos.contains(node))
        return;

    LazyExecutionInfo & lazy_execution_info = lazy_execution_infos[node];
    lazy_execution_info.can_be_lazy_executed = true;

    const ActionsDAGReverseInfo::NodeInfo & node_info = reverse_info.nodes_info[reverse_info.reverse_index.at(node)];

    /// If node is used in result or it doesn't have parents, we can't enable lazy execution.
    if (node_info.used_in_result || node_info.parents.empty() || (node->type != ActionsDAG::ActionType::FUNCTION && node->type != ActionsDAG::ActionType::ALIAS))
    {
        lazy_execution_info.can_be_lazy_executed = false;
        return;
    }

    /// To fill lazy execution info for current node we need to create it for all it's parents.
    for (const auto & parent : node_info.parents)
    {
        setLazyExecutionInfo(parent, reverse_info, short_circuit_nodes, lazy_execution_infos);
        /// Update current node info according to parent info.
        if (short_circuit_nodes.contains(parent))
        {
            /// Use set, because one node can be more than one argument.
            /// Example: expr1 AND expr2 AND expr1.
            std::unordered_set<size_t> indexes;
            auto reordered_args_pos = short_circuit_infos[parent].arguments_position;
            for (size_t i = 0; i != parent->children.size(); ++i)
            {
                if (node == parent->children[reordered_args_pos[i]])
                    indexes.insert(i);
            }

            if (!short_circuit_nodes.at(parent).enable_lazy_execution_for_first_argument && node == parent->children[reordered_args_pos[0]])
            {
                /// We shouldn't add 0 index in node info in this case.
                indexes.erase(0);
                /// Disable lazy execution for current node only if it's disabled for short-circuit node,
                /// because we can have nested short-circuit nodes.
                if (!lazy_execution_infos[parent].can_be_lazy_executed)
                    lazy_execution_info.can_be_lazy_executed = false;
            }

            lazy_execution_info.short_circuit_ancestors_info[parent].insert(indexes.begin(), indexes.end());
        }
        else
            /// If lazy execution is disabled for one of parents, we should disable it for current node.
            lazy_execution_info.can_be_lazy_executed &= lazy_execution_infos[parent].can_be_lazy_executed;

        /// Update info about short-circuit ancestors according to parent info.
        for (const auto & [short_circuit_node, indexes] : lazy_execution_infos[parent].short_circuit_ancestors_info)
            lazy_execution_info.short_circuit_ancestors_info[short_circuit_node].insert(indexes.begin(), indexes.end());
    }

    if (!lazy_execution_info.can_be_lazy_executed)
        return;

    /// Check if current node is common descendant of different function arguments of
    /// short-circuit function that disables lazy execution on this case.
    for (const auto & [short_circuit_node, indexes] : lazy_execution_info.short_circuit_ancestors_info)
    {
        /// If lazy execution is enabled for this short-circuit node,
        /// we shouldn't disable it for current node.
        if (lazy_execution_infos[short_circuit_node].can_be_lazy_executed)
            continue;

        if (!short_circuit_nodes.at(short_circuit_node).enable_lazy_execution_for_common_descendants_of_arguments && indexes.size() > 1)
        {
            lazy_execution_info.can_be_lazy_executed = false;
            return;
        }
    }
}

static DataTypesWithConstInfo getDataTypesWithConstInfoFromNodes(const ActionsDAG::NodeRawConstPtrs & nodes);
bool ExpressionShortCircuitExecuteController::findLazyExecutedNodes(
    const ActionsDAG::NodeRawConstPtrs & children,
    std::unordered_map<const ActionsDAG::Node *, LazyExecutionInfo> & lazy_execution_infos,
    bool force_enable_lazy_execution,
    std::unordered_set<const ActionsDAG::Node *> & lazy_executed_nodes_out)
{
    bool has_lazy_node = false;
    for (const auto * child : children)
    {
        /// Skip node that have already been found as lazy executed.
        if (lazy_executed_nodes_out.contains(child))
        {
            has_lazy_node = true;
            continue;
        }

        /// Skip nodes that cannot be lazy executed.
        if (!lazy_execution_infos[child].can_be_lazy_executed)
            continue;

        /// We cannot propagate lazy execution through arrayJoin, because when we execute
        /// arrayJoin we need to know the exact offset of it's argument to replicate the other arguments.
        /// We cannot determine the exact offset without it's argument execution, because the offset
        /// can depend on on it.
        /// Example: arrayJoin(range(number)), we use lazy execution for masked function execution,
        /// but if we filter column number by mask and then execute function range() and arrayJoin, we will get
        /// the offset that is differ from what we would get without filtering.
        switch (child->type)
        {
            case ActionsDAG::ActionType::FUNCTION: {
                /// Propagate lazy execution through function arguments.
                bool has_lazy_child
                    = findLazyExecutedNodes(child->children, lazy_execution_infos, force_enable_lazy_execution, lazy_executed_nodes_out);

                /// Use lazy execution when:
                ///  - It's force enabled.
                ///  - Function is suitable for lazy execution.
                ///  - Function has lazy executed arguments.
                if (force_enable_lazy_execution || has_lazy_child
                    || child->function_base->isSuitableForShortCircuitArgumentsExecution(
                        getDataTypesWithConstInfoFromNodes(child->children)))
                {
                    has_lazy_node = true;
                    lazy_executed_nodes_out.insert(child);
                }
                break;
            }
            case ActionsDAG::ActionType::ALIAS:
                /// Propagate lazy execution through alias.
                has_lazy_node
                    |= findLazyExecutedNodes(child->children, lazy_execution_infos, force_enable_lazy_execution, lazy_executed_nodes_out);
                break;
            default:
                break;
        }
    }
    return has_lazy_node;
}

void ExpressionShortCircuitExecuteController::markLazyExecutedNodes(const std::unordered_set<const ActionsDAG::Node *> & lazy_executed_nodes)
{
    for (auto & [node, info] : short_circuit_infos)
    {
        if (lazy_executed_nodes.contains(node))
        {
            LOG_TRACE(&Poco::Logger::get("ExpressionShortCircuitExecuteController"), "mark node be lazy executed:{}", node->result_name);
            info.is_lazy_executed = true;
        }
        else
            info.is_lazy_executed = false;
    }
}


ExpressionActions::ExpressionActions(ActionsDAGPtr actions_dag_, const ExpressionActionsSettings & settings_)
    : actions_dag(actions_dag_->clone())
    , settings(settings_)
    , short_circuit_execute_controller(actions_dag, settings_)
{
    linearizeActions();

    if (settings.max_temporary_columns && num_columns > settings.max_temporary_columns)
        throw Exception(ErrorCodes::TOO_MANY_TEMPORARY_COLUMNS,
                        "Too many temporary columns: {}. Maximum: {}",
                        actions_dag->dumpNames(), settings.max_temporary_columns);
}

ExpressionActionsPtr ExpressionActions::clone() const
{
    return std::make_shared<ExpressionActions>(*this);
}

static ActionsDAGReverseInfo getActionsDAGReverseInfo(const std::list<ActionsDAG::Node> & nodes, const ActionsDAG::NodeRawConstPtrs & index)
{
    ActionsDAGReverseInfo result_info;
    result_info.nodes_info.resize(nodes.size());

    for (const auto & node : nodes)
    {
        size_t id = result_info.reverse_index.size();
        result_info.reverse_index[&node] = id;
    }

    for (const auto * node : index)
        result_info.nodes_info[result_info.reverse_index[node]].used_in_result = true;

    for (const auto & node : nodes)
    {
        for (const auto & child : node.children)
            result_info.nodes_info[result_info.reverse_index[child]].parents.emplace_back(&node);
    }

    return result_info;
}

static DataTypesWithConstInfo getDataTypesWithConstInfoFromNodes(const ActionsDAG::NodeRawConstPtrs & nodes)
{
    DataTypesWithConstInfo types;
    types.reserve(nodes.size());
    for (const auto & child : nodes)
    {
        bool is_const = child->column && isColumnConst(*child->column);
        types.push_back({child->result_type, is_const});
    }
    return types;
}

void ExpressionActions::linearizeActions()
{
    /// This function does the topological sort on DAG and fills all the fields of ExpressionActions.
    /// Algorithm traverses DAG starting from nodes without children.
    /// For every node we support the number of created children, and if all children are created, put node into queue.
    struct Data
    {
        const Node * node = nullptr;
        size_t num_created_children = 0;
        ssize_t position = -1;
        size_t num_created_parents = 0;
    };

    const auto & nodes = getNodes();
    const auto & outputs = actions_dag->getOutputs();
    const auto & inputs = actions_dag->getInputs();

    auto reverse_info = getActionsDAGReverseInfo(nodes, outputs);
    std::vector<Data> data;
    for (const auto & node : nodes)
        data.push_back({.node = &node});

    /// There are independent queues for arrayJoin and other actions.
    /// We delay creation of arrayJoin as long as we can, so that they will be executed closer to end.
    std::queue<const Node *> ready_nodes;
    std::queue<const Node *> ready_array_joins;

    for (const auto & node : nodes)
    {
        if (node.children.empty())
            ready_nodes.emplace(&node);
    }

    /// Every argument will have fixed position in columns list.
    /// If argument is removed, it's position may be reused by other action.
    std::stack<size_t> free_positions;

    while (!ready_nodes.empty() || !ready_array_joins.empty())
    {
        auto & stack = ready_nodes.empty() ? ready_array_joins : ready_nodes;
        const Node * node = stack.front();
        stack.pop();

        auto cur_index = reverse_info.reverse_index[node];
        auto & cur = data[cur_index];
        auto & cur_info = reverse_info.nodes_info[cur_index];

        /// Select position for action result.
        size_t free_position = num_columns;
        if (free_positions.empty())
            ++num_columns;
        else
        {
            free_position = free_positions.top();
            free_positions.pop();
        }

        cur.position = free_position;

        ExpressionActions::Arguments arguments;
        arguments.reserve(cur.node->children.size());
        for (const auto * child : cur.node->children)
        {
            auto arg_index = reverse_info.reverse_index[child];
            auto & arg = data[arg_index];
            auto arg_info = reverse_info.nodes_info[arg_index];

            if (arg.position < 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Argument was not calculated for {}", child->result_name);

            ++arg.num_created_parents;

            ExpressionActions::Argument argument;
            argument.pos = arg.position;
            argument.needed_later = arg_info.used_in_result || arg.num_created_parents != arg_info.parents.size();

            if (!argument.needed_later)
                free_positions.push(argument.pos);

            arguments.emplace_back(argument);
        }

        if (node->type == ActionsDAG::ActionType::INPUT)
        {
            /// Argument for input is special. It contains the position from required columns.
            ExpressionActions::Argument argument;
            argument.needed_later = !cur_info.parents.empty();
            arguments.emplace_back(argument);

            //required_columns.push_back({node->result_name, node->result_type});
        }
        actions.push_back({.node = node, .arguments = arguments, .result_position = free_position});

        for (const auto & parent : cur_info.parents)
        {
            auto & parent_data = data[reverse_info.reverse_index[parent]];
            ++parent_data.num_created_children;

            if (parent_data.num_created_children == parent->children.size())
            {
                auto & push_stack = parent->type == ActionsDAG::ActionType::ARRAY_JOIN ? ready_array_joins : ready_nodes;
                push_stack.push(parent);
            }
        }
    }

    result_positions.reserve(outputs.size());

    for (const auto & node : outputs)
    {
        auto pos = data[reverse_info.reverse_index[node]].position;

        if (pos < 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Action for {} was not calculated", node->result_name);

        result_positions.push_back(pos);

        ColumnWithTypeAndName col{node->column, node->result_type, node->result_name};
        sample_block.insert(std::move(col));
    }

    for (const auto * input : inputs)
    {
        const auto & cur = data[reverse_info.reverse_index[input]];
        auto pos = required_columns.size();
        actions[cur.position].arguments.front().pos = pos;
        required_columns.push_back({input->result_name, input->result_type});
        input_positions[input->result_name].emplace_back(pos);
    }
}


static WriteBuffer & operator << (WriteBuffer & out, const ExpressionActions::Argument & argument)
{
    return out << (argument.needed_later ? ": " : ":: ") << argument.pos;
}

std::string ExpressionActions::Action::toString() const
{
    WriteBufferFromOwnString out;
    switch (node->type)
    {
        case ActionsDAG::ActionType::COLUMN:
            out << "COLUMN "
                << (node->column ? node->column->getName() : "(no column)");
            break;

        case ActionsDAG::ActionType::ALIAS:
            out << "ALIAS " << node->children.front()->result_name << " " << arguments.front();
            break;

        case ActionsDAG::ActionType::FUNCTION:
            out << "FUNCTION " << (node->is_function_compiled ? "[compiled] " : "")
                << (node->function_base ? node->function_base->getName() : "(no function)") << "(";
            for (size_t i = 0; i < node->children.size(); ++i)
            {
                if (i)
                    out << ", ";
                out << node->children[i]->result_name << " " << arguments[i];
            }
            out << ")";
            break;

        case ActionsDAG::ActionType::ARRAY_JOIN:
            out << "ARRAY JOIN " << node->children.front()->result_name << " " << arguments.front();
            break;

        case ActionsDAG::ActionType::INPUT:
            out << "INPUT " << arguments.front();
            break;
    }

    out << " -> " << node->result_name
        << " " << (node->result_type ? node->result_type->getName() : "(no type)") << " : " << result_position;
    return out.str();
}

JSONBuilder::ItemPtr ExpressionActions::Action::toTree() const
{
    auto map = std::make_unique<JSONBuilder::JSONMap>();

    if (node)
        node->toTree(*map);

    auto args = std::make_unique<JSONBuilder::JSONArray>();
    auto dropped_args = std::make_unique<JSONBuilder::JSONArray>();
    for (auto arg : arguments)
    {
        args->add(arg.pos);
        if (!arg.needed_later)
            dropped_args->add(arg.pos);
    }

    map->add("Arguments", std::move(args));
    map->add("Removed Arguments", std::move(dropped_args));
    map->add("Result", result_position);

    return map;
}

void ExpressionActions::checkLimits(const ColumnsWithTypeAndName & columns) const
{
    if (settings.max_temporary_non_const_columns)
    {
        size_t non_const_columns = 0;
        for (const auto & column : columns)
            if (column.column && !isColumnConst(*column.column))
                ++non_const_columns;

        if (non_const_columns > settings.max_temporary_non_const_columns)
        {
            WriteBufferFromOwnString list_of_non_const_columns;
            for (const auto & column : columns)
                if (column.column && !isColumnConst(*column.column))
                    list_of_non_const_columns << "\n" << column.name;

            throw Exception(ErrorCodes::TOO_MANY_TEMPORARY_NON_CONST_COLUMNS,
                "Too many temporary non-const columns:{}. Maximum: {}",
                list_of_non_const_columns.str(), settings.max_temporary_non_const_columns);
        }
    }
}

namespace
{
    /// This struct stores context needed to execute actions.
    ///
    /// Execution model is following:
    ///   * execution is performed over list of columns (with fixed size = ExpressionActions::num_columns)
    ///   * every argument has fixed position in columns list, every action has fixed position for result
    ///   * if argument is not needed anymore (Argument::needed_later == false), it is removed from list
    ///   * argument for INPUT is in inputs[inputs_pos[argument.pos]]
    ///
    /// Columns on positions `ExpressionActions::result_positions` are inserted back into block.
    struct ExecutionContext
    {
        ColumnsWithTypeAndName & inputs;
        ColumnsWithTypeAndName columns = {};
        std::vector<ssize_t> inputs_pos = {};
        size_t num_rows = 0;
        ExpressionShortCircuitExecuteController * short_circuit_execute_controller;
    };
}

static void executeAction(const ExpressionActions::Action & action, ExecutionContext & execution_context, bool dry_run)
{
    auto & inputs = execution_context.inputs;
    auto & columns = execution_context.columns;
    auto & num_rows = execution_context.num_rows;

    switch (action.node->type)
    {
        case ActionsDAG::ActionType::FUNCTION:
        {
            auto & res_column = columns[action.result_position];
            if (res_column.type || res_column.column)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Result column is not empty");

            res_column.type = action.node->result_type;
            res_column.name = action.node->result_name;

            if (action.node->column)
            {
                /// Do not execute function if it's result is already known.
                res_column.column = action.node->column->cloneResized(num_rows);
                /// But still need to remove unused arguments.
                for (const auto & argument : action.arguments)
                {
                    if (!argument.needed_later)
                        columns[argument.pos] = {};
                }
                break;
            }

            std::unordered_set<size_t> to_clear_columns;
            ColumnsWithTypeAndName arguments(action.arguments.size());
            auto ordered_arguments = execution_context.short_circuit_execute_controller->getReorderedArgumentsPosition(action.node);
            for (size_t i = 0; i < arguments.size(); ++i)
            {
                auto arg_pos = ordered_arguments[i];
                auto col_pos = action.arguments[arg_pos].pos;
                arguments[i] = columns[col_pos];
                /// Since we may reordered arguments, following case will occur,
                /// original argument list: {arg_0, ref_col = i, needed_later=true}, {arg_1, ref_col = i, needed_later=false}
                /// reordered argument list: {arg_1, ref_col = i, needed_later=false}, {arg_0, ref_col = i, needed_later=true}
                /// we need to lazy clear the columns which is not need anymore.
                if (!action.arguments[arg_pos].needed_later)
                {
                    to_clear_columns.insert(col_pos);
                }
                if (!arguments[i].column)
                {
                    throw Exception(
                        ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK,
                        "Invalid argument. i:{} arg pos:{}, col pos: {}, node: {}",
                        i,
                        arg_pos,
                        col_pos,
                        action.node->result_name);
                }
            }
            for (auto col_pos : to_clear_columns)
            {
                columns[col_pos] = {};
            }

            // Before short circuit functions reorder sampling finish, disable lazy execution.
            if (execution_context.short_circuit_execute_controller->couldLazyExecuted(action.node))
            {
                res_column.column = ColumnFunction::create(
                    num_rows, action.node->function_base, std::move(arguments), true, action.node->is_function_compiled);
            }
            else
            {
                ProfileEvents::increment(ProfileEvents::FunctionExecute);
                if (action.node->is_function_compiled)
                    ProfileEvents::increment(ProfileEvents::CompiledFunctionExecute);
                auto profile_type = execution_context.short_circuit_execute_controller->needProfile(action.node);
                if (profile_type & ExpressionShortCircuitExecuteController::PROFILE_ELAPSED)
                {
                    ExpressionShortCircuitExecuteController::ProfileData profile_data;
                    Stopwatch watch;
                    res_column.column = action.node->function->execute(arguments, res_column.type, num_rows, dry_run);
                    profile_data.elapsed = watch.elapsedNanoseconds();
                    profile_data.sample_rows = res_column.column->size();
                    if (profile_type & ExpressionShortCircuitExecuteController::PROFILE_SELECTIVITY)
                    {
                        IColumn::Filter mask(res_column.column->size(), 1);
                        auto full_column = recursiveRemoveSparse(res_column.column);
                        auto mask_info = extractMask(mask, full_column, nullptr);
                        profile_data.selected_rows = mask_info.ones_count;
                    }
                    else
                    {
                        profile_data.selected_rows = res_column.column->size();
                    }
                    execution_context.short_circuit_execute_controller->addNodeShortCircuitProfile(action.node, profile_data);
                }
                else
                {
                    res_column.column = action.node->function->execute(arguments, res_column.type, num_rows, dry_run);
                }
            }
            break;
        }

        case ActionsDAG::ActionType::ARRAY_JOIN:
        {
            size_t array_join_key_pos = action.arguments.front().pos;
            auto array_join_key = columns[array_join_key_pos];

            /// Remove array join argument in advance if it is not needed.
            if (!action.arguments.front().needed_later)
                columns[array_join_key_pos] = {};

            array_join_key.column = array_join_key.column->convertToFullColumnIfConst();

            const auto * array = getArrayJoinColumnRawPtr(array_join_key.column);
            if (!array)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "ARRAY JOIN of not array nor map: {}", action.node->result_name);

            for (auto & column : columns)
                if (column.column)
                    column.column = column.column->replicate(array->getOffsets());

            for (auto & column : inputs)
                if (column.column)
                    column.column = column.column->replicate(array->getOffsets());

            auto & res_column = columns[action.result_position];

            res_column.column = array->getDataPtr();
            res_column.type = getArrayJoinDataType(array_join_key.type)->getNestedType();
            res_column.name = action.node->result_name;

            num_rows = res_column.column->size();
            break;
        }

        case ActionsDAG::ActionType::COLUMN:
        {
            auto & res_column = columns[action.result_position];
            res_column.column = action.node->column->cloneResized(num_rows);
            res_column.type = action.node->result_type;
            res_column.name = action.node->result_name;
            break;
        }

        case ActionsDAG::ActionType::ALIAS:
        {
            const auto & arg = action.arguments.front();
            if (action.result_position != arg.pos)
            {
                columns[action.result_position].column = columns[arg.pos].column;
                columns[action.result_position].type = columns[arg.pos].type;

                if (!arg.needed_later)
                    columns[arg.pos] = {};
            }

            columns[action.result_position].name = action.node->result_name;

            break;
        }

        case ActionsDAG::ActionType::INPUT:
        {
            auto pos = execution_context.inputs_pos[action.arguments.front().pos];
            if (pos < 0)
            {
                /// Here we allow to skip input if it is not in block (in case it is not needed).
                /// It may be unusual, but some code depend on such behaviour.
                if (action.arguments.front().needed_later)
                    throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK,
                                    "Not found column {} in block",
                                    action.node->result_name);
            }
            else
                columns[action.result_position] = std::move(inputs[pos]);

            break;
        }
    }
}

void ExpressionActions::execute(Block & block, size_t & num_rows, bool dry_run)
{
    /// For safety, when exception occurs in adaptive mode, we rollback to normal mode and run again.
    size_t need_to_sample_rows = short_circuit_execute_controller.needSampleRows();
    if (short_circuit_execute_controller.hasReorderableShortCircuitFunctions() && need_to_sample_rows && num_rows > need_to_sample_rows)
    {
        try
        {
            Block to_sample_block;
            for (const auto & col : block.getColumnsWithTypeAndName())
            {
                auto sample_col = col.column->cut(0, need_to_sample_rows);
                to_sample_block.insert({sample_col, col.type, col.name});
            }
            // All the functions should be stateless here.
            size_t tmp_num_rows = need_to_sample_rows;
            executeImpl(to_sample_block, tmp_num_rows, dry_run);
            short_circuit_execute_controller.tryReorderShortCircuitFunctionsAguments(need_to_sample_rows);
        }
        catch (...)
        {
            short_circuit_execute_controller.disableAdaptiveReorderArguments();
        }
        executeImpl(block, num_rows, dry_run);
    }
    else
    {
        bool rerun = false;
        bool in_adaptive_reorder_mode = short_circuit_execute_controller.isEnableAdaptiveReorderArguments();
        Block tmp_block = block;
        size_t tmp_num_rows = num_rows;
        try
        {
            executeImpl(tmp_block, tmp_num_rows, dry_run);
            block.swap(tmp_block);
            num_rows = tmp_num_rows;
        }
        catch (...)
        {
            if (in_adaptive_reorder_mode)
            {
                short_circuit_execute_controller.disableAdaptiveReorderArguments();
                rerun = true;
            }
            else
                throw;
        }
        if (rerun)
        {
            executeImpl(block, num_rows, dry_run);
        }
        else
        {
            short_circuit_execute_controller.tryReorderShortCircuitFunctionsAguments(num_rows);
        }
    }
}

void ExpressionActions::executeImpl(Block & block, size_t & num_rows, bool dry_run)
{
    ExecutionContext execution_context
    {
        .inputs = block.data,
        .num_rows = num_rows,
        .short_circuit_execute_controller = &short_circuit_execute_controller,
    };

    execution_context.inputs_pos.assign(required_columns.size(), -1);

    for (size_t pos = 0; pos < block.columns(); ++pos)
    {
        const auto & col = block.getByPosition(pos);
        auto it = input_positions.find(col.name);
        if (it != input_positions.end())
        {
            for (auto input_pos : it->second)
            {
                if (execution_context.inputs_pos[input_pos] < 0)
                {
                    execution_context.inputs_pos[input_pos] = pos;
                    break;
                }
            }
        }
    }

    execution_context.columns.resize(num_columns);

    {
        for (const auto & action : actions)
        {
            try
            {
                executeAction(action, execution_context, dry_run);
                checkLimits(execution_context.columns);

                //std::cerr << "Action: " << action.toString() << std::endl;
                //for (const auto & col : execution_context.columns)
                //    std::cerr << col.dumpStructure() << std::endl;
            }
            catch (Exception & e)
            {
                e.addMessage(fmt::format("while executing '{}'", action.toString()));
                throw;
            }
        }
    }
    if (actions_dag->isInputProjected())
    {
        block.clear();
    }
    else
    {
        ::sort(execution_context.inputs_pos.rbegin(), execution_context.inputs_pos.rend());
        for (auto input : execution_context.inputs_pos)
            if (input >= 0)
                block.erase(input);
    }

    Block res;

    for (auto pos : result_positions)
        if (execution_context.columns[pos].column)
            res.insert(execution_context.columns[pos]);

    for (auto && item : block)
        res.insert(std::move(item));

    block.swap(res);

    num_rows = execution_context.num_rows;
}

void ExpressionActions::execute(Block & block, bool dry_run)
{
    size_t num_rows = block.rows();

    execute(block, num_rows, dry_run);

    if (!block)
        block.insert({DataTypeUInt8().createColumnConst(num_rows, 0), std::make_shared<DataTypeUInt8>(), "_dummy"});
}

Names ExpressionActions::getRequiredColumns() const
{
    Names names;
    for (const auto & input : required_columns)
        names.push_back(input.name);
    return names;
}

bool ExpressionActions::hasArrayJoin() const
{
    return getActionsDAG().hasArrayJoin();
}

void ExpressionActions::assertDeterministic() const
{
    getActionsDAG().assertDeterministic();
}


NameAndTypePair ExpressionActions::getSmallestColumn(const NamesAndTypesList & columns)
{
    std::optional<size_t> min_size;
    NameAndTypePair result;

    for (const auto & column : columns)
    {
        /// Skip .sizeX and similar meta information
        if (column.isSubcolumn())
            continue;

        /// @todo resolve evil constant
        size_t size = column.type->haveMaximumSizeOfValue() ? column.type->getMaximumSizeOfValueInMemory() : 100;

        if (!min_size || size < *min_size)
        {
            min_size = size;
            result = column;
        }
    }

    if (!min_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No available columns");

    return result;
}

std::string ExpressionActions::dumpActions() const
{
    WriteBufferFromOwnString ss;

    ss << "input:\n";
    for (const auto & input_column : required_columns)
        ss << input_column.name << " " << input_column.type->getName() << "\n";

    ss << "\nactions:\n";
    for (const auto & action : actions)
        ss << action.toString() << '\n';

    ss << "\noutput:\n";
    NamesAndTypesList output_columns = sample_block.getNamesAndTypesList();
    for (const auto & output_column : output_columns)
        ss << output_column.name << " " << output_column.type->getName() << "\n";

    ss << "\nproject input: " << actions_dag->isInputProjected() << "\noutput positions:";
    for (auto pos : result_positions)
        ss << " " << pos;
    ss << "\n";

    return ss.str();
}

void ExpressionActions::describeActions(WriteBuffer & out, std::string_view prefix) const
{
    bool first = true;
    for (const auto & action : actions)
    {
        out << prefix << (first ? "Actions: " : "         ");
        out << action.toString() << '\n';
        first = false;
    }

    out << prefix << "Positions:";
    for (const auto & pos : result_positions)
        out << ' ' << pos;
    out << '\n';
}

JSONBuilder::ItemPtr ExpressionActions::toTree() const
{
    auto inputs_array = std::make_unique<JSONBuilder::JSONArray>();

    for (const auto & input_column : required_columns)
    {
        auto map = std::make_unique<JSONBuilder::JSONMap>();
        map->add("Name", input_column.name);
        if (input_column.type)
            map->add("Type", input_column.type->getName());

        inputs_array->add(std::move(map));
    }

    auto outputs_array = std::make_unique<JSONBuilder::JSONArray>();

    for (const auto & output_column : sample_block)
    {
        auto map = std::make_unique<JSONBuilder::JSONMap>();
        map->add("Name", output_column.name);
        if (output_column.type)
            map->add("Type", output_column.type->getName());

        outputs_array->add(std::move(map));
    }

    auto actions_array = std::make_unique<JSONBuilder::JSONArray>();
    {
        for (const auto & action : actions)
            actions_array->add(action.toTree());
    }

    auto positions_array = std::make_unique<JSONBuilder::JSONArray>();
    for (auto pos : result_positions)
        positions_array->add(pos);

    auto map = std::make_unique<JSONBuilder::JSONMap>();
    map->add("Inputs", std::move(inputs_array));
    map->add("Actions", std::move(actions_array));
    map->add("Outputs", std::move(outputs_array));
    map->add("Positions", std::move(positions_array));
    map->add("Project Input", actions_dag->isInputProjected());

    return map;
}

bool ExpressionActions::checkColumnIsAlwaysFalse(const String & column_name) const
{
    /// Check has column in (empty set).
    String set_to_check;

    for (auto it = actions.rbegin(); it != actions.rend(); ++it)
    {
        const auto & action = *it;
        if (action.node->type == ActionsDAG::ActionType::FUNCTION && action.node->function_base)
        {
            if (action.node->result_name == column_name && action.node->children.size() > 1)
            {
                auto name = action.node->function_base->getName();
                if ((name == "in" || name == "globalIn"))
                {
                    set_to_check = action.node->children[1]->result_name;
                    break;
                }
            }
        }
    }

    if (!set_to_check.empty())
    {
        for (const auto & action : actions)
        {
            if (action.node->type == ActionsDAG::ActionType::COLUMN && action.node->result_name == set_to_check)
                // Constant ColumnSet cannot be empty, so we only need to check non-constant ones.
                if (const auto * column_set = checkAndGetColumn<const ColumnSet>(action.node->column.get()))
                    if (auto future_set = column_set->getData())
                        if (auto set = future_set->get())
                            if (set->getTotalRowCount() == 0)
                                return true;
        }
    }

    return false;
}

void ExpressionActionsChain::addStep(NameSet non_constant_inputs)
{
    if (steps.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add action to empty ExpressionActionsChain");

    ColumnsWithTypeAndName columns = steps.back()->getResultColumns();
    for (auto & column : columns)
        if (column.column && isColumnConst(*column.column) && non_constant_inputs.contains(column.name))
            column.column = nullptr;

    steps.push_back(std::make_unique<ExpressionActionsStep>(std::make_shared<ActionsDAG>(columns)));
}

void ExpressionActionsChain::finalize()
{
    /// Finalize all steps. Right to left to define unnecessary input columns.
    for (int i = static_cast<int>(steps.size()) - 1; i >= 0; --i)
    {
        auto & required_output = steps[i]->required_output;
        NameSet required_names;
        for (const auto & output : required_output)
            required_names.insert(output.first);

        if (i + 1 < static_cast<int>(steps.size()))
        {
            const NameSet & additional_input = steps[i + 1]->additional_input;
            for (const auto & it : steps[i + 1]->getRequiredColumns())
            {
                if (!additional_input.contains(it.name))
                {
                    auto iter = required_output.find(it.name);
                    if (iter == required_output.end())
                        required_names.insert(it.name);
                    else
                        iter->second = false;
                }
            }
        }
        steps[i]->finalize(required_names);
    }

    /// Adding the ejection of unnecessary columns to the beginning of each step.
    for (size_t i = 1; i < steps.size(); ++i)
    {
        size_t columns_from_previous = steps[i - 1]->getResultColumns().size();

        /// If unnecessary columns are formed at the output of the previous step, we'll add them to the beginning of this step.
        /// Except when we drop all the columns and lose the number of rows in the block.
        if (!steps[i]->getResultColumns().empty()
            && columns_from_previous > steps[i]->getRequiredColumns().size())
            steps[i]->prependProjectInput();
    }
}

std::string ExpressionActionsChain::dumpChain() const
{
    WriteBufferFromOwnString ss;

    for (size_t i = 0; i < steps.size(); ++i)
    {
        ss << "step " << i << "\n";
        ss << "required output:\n";
        for (const auto & it : steps[i]->required_output)
            ss << it.first << "\n";
        ss << "\n" << steps[i]->dump() << "\n";
    }

    return ss.str();
}

ExpressionActionsChain::ArrayJoinStep::ArrayJoinStep(ArrayJoinActionPtr array_join_, ColumnsWithTypeAndName required_columns_)
    : Step({})
    , array_join(std::move(array_join_))
    , result_columns(std::move(required_columns_))
{
    for (auto & column : result_columns)
    {
        required_columns.emplace_back(NameAndTypePair(column.name, column.type));

        if (array_join->columns.contains(column.name))
        {
            const auto & array = getArrayJoinDataType(column.type);
            column.type = array->getNestedType();
            /// Arrays are materialized
            column.column = nullptr;
        }
    }
}

void ExpressionActionsChain::ArrayJoinStep::finalize(const NameSet & required_output_)
{
    NamesAndTypesList new_required_columns;
    ColumnsWithTypeAndName new_result_columns;

    for (const auto & column : result_columns)
    {
        if (array_join->columns.contains(column.name) || required_output_.contains(column.name))
            new_result_columns.emplace_back(column);
    }
    for (const auto & column : required_columns)
    {
        if (array_join->columns.contains(column.name) || required_output_.contains(column.name))
            new_required_columns.emplace_back(column);
    }

    std::swap(required_columns, new_required_columns);
    std::swap(result_columns, new_result_columns);
}

ExpressionActionsChain::JoinStep::JoinStep(
    std::shared_ptr<TableJoin> analyzed_join_,
    JoinPtr join_,
    const ColumnsWithTypeAndName & required_columns_)
    : Step({})
    , analyzed_join(std::move(analyzed_join_))
    , join(std::move(join_))
{
    for (const auto & column : required_columns_)
        required_columns.emplace_back(column.name, column.type);

    result_columns = required_columns_;
    analyzed_join->addJoinedColumnsAndCorrectTypes(result_columns, true);
}

void ExpressionActionsChain::JoinStep::finalize(const NameSet & required_output_)
{
    /// We need to update required and result columns by removing unused ones.
    NamesAndTypesList new_required_columns;
    ColumnsWithTypeAndName new_result_columns;

    /// That's an input columns we need.
    NameSet required_names = required_output_;
    for (const auto & name : analyzed_join->getAllNames(JoinTableSide::Left))
        required_names.emplace(name);

    for (const auto & onexpr : analyzed_join->getClauses())
        if (const auto & cond_name = onexpr.condColumnNames().first; !cond_name.empty())
            required_names.emplace(cond_name);

    for (const auto & column : required_columns)
    {
        if (required_names.contains(column.name))
            new_required_columns.emplace_back(column);
    }

    /// Result will also contain joined columns.
    for (const auto & column : analyzed_join->columnsAddedByJoin())
        required_names.emplace(column.name);

    for (const auto & column : result_columns)
    {
        if (required_names.contains(column.name))
            new_result_columns.emplace_back(column);
    }

    std::swap(required_columns, new_required_columns);
    std::swap(result_columns, new_result_columns);
}

ActionsDAGPtr & ExpressionActionsChain::Step::actions()
{
    return typeid_cast<ExpressionActionsStep &>(*this).actions_dag;
}

const ActionsDAGPtr & ExpressionActionsChain::Step::actions() const
{
    return typeid_cast<const ExpressionActionsStep &>(*this).actions_dag;
}

}
