#include <iostream>

#include <Interpreters/AggregationCommon.h>

#include <Common/HashTable/SmallTable.h>
#include <Common/HashTable/hash_table8.hpp>

using namespace std;

class Timer
{
public:
    void start()
    {
        m_StartTime = std::chrono::system_clock::now();
        m_bRunning = true;
    }

    void stop()
    {
        m_EndTime = std::chrono::system_clock::now();
        m_bRunning = false;
    }

    double elapsedMilliseconds()
    {
        std::chrono::time_point<std::chrono::system_clock> endTime;

        if(m_bRunning)
        {
            endTime = std::chrono::system_clock::now();
        }
        else
        {
            endTime = m_EndTime;
        }

        return std::chrono::duration_cast<std::chrono::milliseconds>(endTime - m_StartTime).count();
    }

    double elapsedSeconds()
    {
        return elapsedMilliseconds() / 1000.0;
    }

private:
    std::chrono::time_point<std::chrono::system_clock> m_StartTime;
    std::chrono::time_point<std::chrono::system_clock> m_EndTime;
    bool                                               m_bRunning = false;
};

int main(int, char **)
{
    /*
    {
        using Cont = SmallSet<int, 16>;
        Cont cont;

        cont.insert(1);
        cont.insert(2);

        Cont::iterator it;
        bool inserted;

        cont.emplace(3, it, inserted);
        std::cerr << inserted << ", " << it->getValue() << std::endl;

        cont.emplace(3, it, inserted);
        std::cerr << inserted << ", " << it->getValue() << std::endl;

        for (auto x : cont)
            std::cerr << x.getValue() << std::endl;

        DB::WriteBufferFromOwnString wb;
        cont.write(wb);

        std::cerr << "dump: " << wb.str() << std::endl;
    }

    {
        using Cont = SmallSet<DB::UInt128, 16>;
        Cont cont;

        DB::WriteBufferFromOwnString wb;
        cont.write(wb);

        std::cerr << "dump: " << wb.str() << std::endl;
    }
    */

    using Map = HashMap<int64_t, int64_t>;
    emhash8::HashMap<int64_t, int64_t> emhash_tbl;
    Map ck_tbl;
    auto n = 100000000;
    Timer t;
    t.start();
    for (auto i = 0; i < n; ++i)
        emhash_tbl.insert(std::pair<int64_t, int64_t>(i, i));
    t.stop();
    cout << "em8 insert " << t.elapsedMilliseconds() << " ms on " << n << " elements." << endl;

    t.start();
    for (auto i = 0; i < n; ++i)
    {
       Map::LookupResult it;
       bool inserted;
       ck_tbl.emplace(i, it, inserted, ck_tbl.hash(i));
       if (inserted)
           it->getMapped() = i;
    }
    t.stop();
    cout << "ck insert " << t.elapsedMilliseconds() << " ms on " << n << " elements." << endl;

    t.start();
    int64_t sum = 0;
    for (auto i = 0; i < n; ++i)
	    sum += emhash_tbl.find(i)->second;
    t.stop();
    cout << "em8 find " << t.elapsedMilliseconds() << " ms, sum " << sum << endl;

    t.start();
    sum = 0;
    for (auto i = 0; i < n; ++i)
    {
	    auto * it = ck_tbl.find(i);
        sum += it->getMapped();
    }
    t.stop();
    cout << "ck find " << t.elapsedMilliseconds() << " ms, sum " << sum << endl;

    return 0;
}
