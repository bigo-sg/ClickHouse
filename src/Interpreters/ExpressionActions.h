#pragma once

#include <Core/Block.h>
#include <Core/ColumnNumbers.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActionsSettings.h>

#include <unordered_map>
#include <variant>
#include <shared_mutex>

#include "config.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class TableJoin;
class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

class ArrayJoinAction;
using ArrayJoinActionPtr = std::shared_ptr<ArrayJoinAction>;

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

struct ActionsDAGReverseInfo
{
    struct NodeInfo
    {
        std::vector<const ActionsDAG::Node *> parents;
        bool used_in_result = false;
    };

    using ReverseIndex = std::unordered_map<const ActionsDAG::Node *, size_t>;
    std::vector<NodeInfo> nodes_info;
    ReverseIndex reverse_index;
};

/// Information about the node that helps to determine if it can be executed lazily.
struct LazyExecutionInfo
{
    bool can_be_lazy_executed;
    /// For each node we need to know all it's ancestors that are short-circuit functions.
    /// Also we need to know which arguments of this short-circuit functions are ancestors for the node
    /// (we will store the set of indexes of arguments), because for some short-circuit function we shouldn't
    /// enable lazy execution for nodes that are common descendants of different function arguments.
    /// Example: if(cond, expr1(..., expr, ...), expr2(..., expr, ...))).
    std::unordered_map<const ActionsDAG::Node *, std::unordered_set<size_t>> short_circuit_ancestors_info;
};

/// To determine whether a node could be executed lazily.
/// Let's make it clear that in which situations a node could not be executed lazily.
/// 1. it's not a function or alias node
/// 2. it doesn't have any parent.
/// 3. it is used as a output attribute of this ExpressionActions.
/// 4. it's the first node of one of its parent, and the parent's
///    enable_lazy_execution_for_first_argument = false.
/// 5. it's a common descendant of different function arguments of short-circuit functions
/// 6. it's parent could not be executed lazily and the parent is not a short-circuit function.
class ExpressionShortCircuitExecuteController
{
public:
    struct ProfileData
    {
        /// How many rows are sampled at present.
        UInt64 sample_rows = 0;
        /// After execution, the number of rows that meet the condition
        UInt64 selected_rows = 0;
        /// The total execution time of this node for sample rows
        UInt64 elapsed = 0;
    };

    enum ProfileType
    {
        /// Not profile this node
        NOT_PROFILE = 0,
        /// Profile this node's elapsed time.
        PROFILE_ELAPSED = 1,
        /// If this node is a argument of a short-circuit function, profile this node's selectivity.
        /// The selectivity is the ratio of the number of rows that meet the condition to the total number of rows.
        PROFILE_SELECTIVITY = 2,
    };

    explicit ExpressionShortCircuitExecuteController(
        const ActionsDAGPtr & actions_dag_,
        const ExpressionActionsSettings & settings);

    ExpressionShortCircuitExecuteController(const ExpressionShortCircuitExecuteController & b)
        : actions_dag(b.actions_dag)
        , short_circuit_function_evaluation(b.short_circuit_function_evaluation)
        , enable_adaptive_reorder_arguments(b.enable_adaptive_reorder_arguments)
        , sampled_rows(b.sampled_rows.load())
        , finished_adaptive_reorder_arguements(b.finished_adaptive_reorder_arguements.load())
        , short_circuit_infos(b.short_circuit_infos)
        , has_reorderable_short_circuit_functions(b.has_reorderable_short_circuit_functions)
    {}

    ExpressionShortCircuitExecuteController & operator=(const ExpressionShortCircuitExecuteController & b)
    {
        actions_dag = b.actions_dag;
        short_circuit_function_evaluation = b.short_circuit_function_evaluation;
        enable_adaptive_reorder_arguments = b.enable_adaptive_reorder_arguments;
        sampled_rows = b.sampled_rows.load();
        finished_adaptive_reorder_arguements = b.finished_adaptive_reorder_arguements.load();
        short_circuit_infos = b.short_circuit_infos;
        has_reorderable_short_circuit_functions = b.has_reorderable_short_circuit_functions;
        return *this;
    }

    /// Determine if this action should be executed lazily. If it should and the node type is FUNCTION, then the function
    /// won't be executed and will be stored with it's arguments in ColumnFunction with isShortCircuitArgument() = true.
    bool couldLazyExecuted(const ActionsDAG::Node * node);
    int  needProfile(const ActionsDAG::Node * node);
    void addNodeShortCircuitProfile(const ActionsDAG::Node * node, const ProfileData & profile_data_);
    void tryReorderShortCircuitFunctionsAguments(size_t num_rows);
    std::vector<size_t> getReorderedArgumentsPosition(const ActionsDAG::Node * node);
    size_t needSampleRows() const;
    inline bool hasReorderableShortCircuitFunctions() const { return has_reorderable_short_circuit_functions; }

    /// disable adaptive mode, and rollback the arguments positions.
    void disableAdaptiveReorderArguments();
    inline bool isEnableAdaptiveReorderArguments() const { return enable_adaptive_reorder_arguments; }

private:
    struct ShortCircuitInfo
    {
        ProfileData profile_data;
        bool is_lazy_executed = false;
        std::vector<size_t> arguments_position;
        bool is_short_circuit_function_child = false;
    };

    std::shared_mutex mutex;

    ActionsDAGPtr actions_dag;
    ShortCircuitFunctionEvaluation short_circuit_function_evaluation;
    /// By enable adaptive reorder arguments of short circuit functions, we sample the first max_sample_rows rows execute cost.
    /// This will bring some cost, but it will be amortized in the future.
    bool enable_adaptive_reorder_arguments;
    /// Sample enough rows to determine whether to reorder the arguments of short circuit functions.
    size_t max_sample_rows = 512;
    /// If a function has too many arguments, we will not reorder the arguments of this function.
    /// Since it may cause serious performance degradation by executing all expressions at sample stage.
    size_t max_allowed_arguments = 128;
    std::atomic<UInt64> sampled_rows = 0;
    std::atomic<bool> finished_adaptive_reorder_arguements = false;
    std::unordered_map<const ActionsDAG::Node *, ShortCircuitInfo> short_circuit_infos;
    bool has_reorderable_short_circuit_functions = false;

    double calculateNodeElapsed(const ActionsDAG::Node * node);
    void reorderShortCircuitFunctionsAguments();
    std::unordered_set<const ActionsDAG::Node *>  processShortCircuitFunctions();
    void setLazyExecutionInfo(
        const ActionsDAG::Node * node,
        const ActionsDAGReverseInfo & reverse_info,
        const std::unordered_map<const ActionsDAG::Node *, IFunctionBase::ShortCircuitSettings> & short_circuit_nodes,
        std::unordered_map<const ActionsDAG::Node *, LazyExecutionInfo> & lazy_execution_infos);
    bool findLazyExecutedNodes(
        const ActionsDAG::NodeRawConstPtrs & children,
        std::unordered_map<const ActionsDAG::Node *, LazyExecutionInfo> & lazy_execution_infos,
        bool force_enable_lazy_execution,
        std::unordered_set<const ActionsDAG::Node *> & lazy_executed_nodes_out);
    void markLazyExecutedNodes(const std::unordered_set<const ActionsDAG::Node *> & lazy_executed_nodes);
};

/// Sequence of actions on the block.
/// Is used to calculate expressions.
///
/// Takes ActionsDAG and orders actions using top-sort.
class ExpressionActions
{
public:

    using Node = ActionsDAG::Node;

    struct Argument
    {
        /// Position in ExecutionContext::columns
        size_t pos = 0;
        /// True if there is another action which will use this column.
        /// Otherwise column will be removed.
        bool needed_later = false;
    };

    using Arguments = std::vector<Argument>;

    struct Action
    {
        const Node * node;
        Arguments arguments;
        size_t result_position;

        std::string toString() const;
        JSONBuilder::ItemPtr toTree() const;
    };

    using Actions = std::vector<Action>;

    /// This map helps to find input position by it's name.
    /// Key is a view to input::result_name.
    /// Result is a list because it is allowed for inputs to have same names.
    using NameToInputMap = std::unordered_map<std::string_view, std::list<size_t>>;

private:
    ActionsDAGPtr actions_dag;
    Actions actions;
    size_t num_columns = 0;

    NamesAndTypesList required_columns;
    NameToInputMap input_positions;
    ColumnNumbers result_positions;
    Block sample_block;

    ExpressionActionsSettings settings;
    ExpressionShortCircuitExecuteController short_circuit_execute_controller;

public:
    ExpressionActions() = delete;
    explicit ExpressionActions(ActionsDAGPtr actions_dag_, const ExpressionActionsSettings & settings_ = {});
    ExpressionActions(const ExpressionActions &) = default;
    ExpressionActions & operator=(const ExpressionActions &) = default;

    const Actions & getActions() const { return actions; }
    const std::list<Node> & getNodes() const { return actions_dag->getNodes(); }
    const ActionsDAG & getActionsDAG() const { return *actions_dag; }
    const ColumnNumbers & getResultPositions() const { return result_positions; }
    const ExpressionActionsSettings & getSettings() const { return settings; }

    /// Get a list of input columns.
    Names getRequiredColumns() const;
    const NamesAndTypesList & getRequiredColumnsWithTypes() const { return required_columns; }

    /// Execute the expression on the block. The block must contain all the columns returned by getRequiredColumns.
    void execute(Block & block, size_t & num_rows, bool dry_run = false);
    /// The same, but without `num_rows`. If result block is empty, adds `_dummy` column to keep block size.
    void execute(Block & block, bool dry_run = false);

    bool hasArrayJoin() const;
    void assertDeterministic() const;

    /// Obtain a sample block that contains the names and types of result columns.
    const Block & getSampleBlock() const { return sample_block; }

    std::string dumpActions() const;

    void describeActions(WriteBuffer & out, std::string_view prefix) const;

    JSONBuilder::ItemPtr toTree() const;

    static NameAndTypePair getSmallestColumn(const NamesAndTypesList & columns);

    /// Check if column is always zero. True if it's definite, false if we can't say for sure.
    /// Call it only after subqueries for sets were executed.
    bool checkColumnIsAlwaysFalse(const String & column_name) const;

    ExpressionActionsPtr clone() const;

private:
    void checkLimits(const ColumnsWithTypeAndName & columns) const;

    void linearizeActions();

    void executeImpl(Block & block, size_t & num_rows, bool dry_run = false);
};


/** The sequence of transformations over the block.
  * It is assumed that the result of each step is fed to the input of the next step.
  * Used to execute parts of the query individually.
  *
  * For example, you can create a chain of two steps:
  *     1) evaluate the expression in the WHERE clause,
  *     2) calculate the expression in the SELECT section,
  * and between the two steps do the filtering by value in the WHERE clause.
  */
struct ExpressionActionsChain : WithContext
{
    explicit ExpressionActionsChain(ContextPtr context_) : WithContext(context_) {}


    struct Step
    {
        virtual ~Step() = default;
        explicit Step(Names required_output_)
        {
            for (const auto & name : required_output_)
                required_output[name] = true;
        }

        /// Columns were added to the block before current step in addition to prev step output.
        NameSet additional_input;
        /// Columns which are required in the result of current step.
        /// Flag is true if column from required_output is needed only for current step and not used in next actions
        /// (and can be removed from block). Example: filter column for where actions.
        /// If not empty, has the same size with required_output; is filled in finalize().
        std::unordered_map<std::string, bool> required_output;

        void addRequiredOutput(const std::string & name) { required_output[name] = true; }

        virtual NamesAndTypesList getRequiredColumns() const = 0;
        virtual ColumnsWithTypeAndName getResultColumns() const = 0;
        /// Remove unused result and update required columns
        virtual void finalize(const NameSet & required_output_) = 0;
        /// Add projections to expression
        virtual void prependProjectInput() const = 0;
        virtual std::string dump() const = 0;

        /// Only for ExpressionActionsStep
        ActionsDAGPtr & actions();
        const ActionsDAGPtr & actions() const;
    };

    struct ExpressionActionsStep : public Step
    {
        ActionsDAGPtr actions_dag;

        explicit ExpressionActionsStep(ActionsDAGPtr actions_dag_, Names required_output_ = Names())
            : Step(std::move(required_output_))
            , actions_dag(std::move(actions_dag_))
        {
        }

        NamesAndTypesList getRequiredColumns() const override
        {
            return actions_dag->getRequiredColumns();
        }

        ColumnsWithTypeAndName getResultColumns() const override
        {
            return actions_dag->getResultColumns();
        }

        void finalize(const NameSet & required_output_) override
        {
            if (!actions_dag->isOutputProjected())
                actions_dag->removeUnusedActions(required_output_);
        }

        void prependProjectInput() const override
        {
            actions_dag->projectInput();
        }

        std::string dump() const override
        {
            return actions_dag->dumpDAG();
        }
    };

    struct ArrayJoinStep : public Step
    {
        ArrayJoinActionPtr array_join;
        NamesAndTypesList required_columns;
        ColumnsWithTypeAndName result_columns;

        ArrayJoinStep(ArrayJoinActionPtr array_join_, ColumnsWithTypeAndName required_columns_);

        NamesAndTypesList getRequiredColumns() const override { return required_columns; }
        ColumnsWithTypeAndName getResultColumns() const override { return result_columns; }
        void finalize(const NameSet & required_output_) override;
        void prependProjectInput() const override {} /// TODO: remove unused columns before ARRAY JOIN ?
        std::string dump() const override { return "ARRAY JOIN"; }
    };

    struct JoinStep : public Step
    {
        std::shared_ptr<TableJoin> analyzed_join;
        JoinPtr join;

        NamesAndTypesList required_columns;
        ColumnsWithTypeAndName result_columns;

        JoinStep(std::shared_ptr<TableJoin> analyzed_join_, JoinPtr join_, const ColumnsWithTypeAndName & required_columns_);
        NamesAndTypesList getRequiredColumns() const override { return required_columns; }
        ColumnsWithTypeAndName getResultColumns() const override { return result_columns; }
        void finalize(const NameSet & required_output_) override;
        void prependProjectInput() const override {} /// TODO: remove unused columns before JOIN ?
        std::string dump() const override { return "JOIN"; }
    };

    using StepPtr = std::unique_ptr<Step>;
    using Steps = std::vector<StepPtr>;

    Steps steps;

    void addStep(NameSet non_constant_inputs = {});

    void finalize();

    void clear()
    {
        steps.clear();
    }

    ActionsDAGPtr getLastActions(bool allow_empty = false)
    {
        if (steps.empty())
        {
            if (allow_empty)
                return {};
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty ExpressionActionsChain");
        }

        return typeid_cast<ExpressionActionsStep *>(steps.back().get())->actions_dag;
    }

    Step & getLastStep()
    {
        if (steps.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty ExpressionActionsChain");

        return *steps.back();
    }

    Step & lastStep(const NamesAndTypesList & columns)
    {
        if (steps.empty())
            steps.emplace_back(std::make_unique<ExpressionActionsStep>(std::make_shared<ActionsDAG>(columns)));
        return *steps.back();
    }

    std::string dumpChain() const;
};

}
