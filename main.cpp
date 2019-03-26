#include <chrono>
#include <csignal>
#include <iostream>
#include <librados.hpp>
#include <map>
#include <set>
#include <string>
#include <fstream>
#include <thread>
#include <vector>
#include <system_error>

#include "mysignals.h"
#include "radosutil.h"

using namespace librados;
using namespace std;
using namespace chrono;

struct bench_settings
{
  string pool;
  string mode;
  string specific_bench_item;
  int threads;
  int secs;
  size_t object_size;
  size_t block_size;
};

template <class T> static double dur2sec(const T &dur)
{
  return duration_cast<duration<double>>(dur).count();
}

template <class T> static double dur2msec(const T &dur)
{
  return duration_cast<duration<double, milli>>(dur).count();
}

template <class T> static uint64_t dur2nsec(const T &dur)
{
  return duration_cast<duration<uint64_t, nano>>(dur).count();
}

template <class T>
static void print_breakdown(const vector<T> &all_ops, size_t thread_count)
{
  T totaltime(0);

  map<size_t, size_t> dur2count;
  map<size_t, T> dur2totaltime;

  T mindur(minutes(42));
  T maxdur(0);
  size_t maxcount = 0;
  for (const auto &res : all_ops)
  {
    totaltime += res;
    if (res > maxdur)
      maxdur = res;
    if (res < mindur)
      mindur = res;

    const auto nsec = dur2nsec(res);
    size_t baserange = 10;
    while (nsec >= baserange)
      baserange *= 10;
    baserange /= 10;
    const size_t range = (nsec / baserange) * baserange;

    const auto cnt = ++(dur2count[range]);
    if (cnt > maxcount)
      maxcount = cnt;

    dur2totaltime[range] += res;
  }

  cout << "min latency " << dur2msec(mindur) << " ms" << endl;
  cout << "max latency " << dur2msec(maxdur) << " ms" << endl;

  const size_t maxbarsize = 30;

  for (const auto &p : dur2count)
  {
    const auto &nsecgrp = p.first;
    const auto &count = p.second;
    const auto barsize = count * maxbarsize / maxcount;

    auto bar = string(barsize, '#') + string(maxbarsize - barsize, ' ');
    cout << ">=" << setw(5) << nsecgrp / 1000000.0;
    cout << " ms: " << setw(3) << count * 100 / all_ops.size() << "% " << bar;
    cout << " cnt=" << count << endl;
  }

  cout << "Average iops: " << (all_ops.size() * thread_count / dur2sec(totaltime)) << endl;

  cout << "Average latency: " << (dur2msec(totaltime) / all_ops.size()) << " ms" << endl;

  cout << "Total writes: " << all_ops.size() << endl;

  if (thread_count > 1)
    cout << "iops per thread: " << (all_ops.size() / dur2sec(totaltime)) << endl;
}

static void fill_urandom(char *buf, size_t len)
{
  ifstream infile;
  infile.exceptions(ifstream::failbit | ifstream::badbit);
  infile.open("/dev/urandom", ios::binary | ios::in);
  infile.read(buf, len);
}

// May be called in a thread.
static void _do_bench(
    const unique_ptr<bench_settings> &settings,
    const vector<string> &obj_names,
    IoCtx &ioctx,
    vector<steady_clock::duration> &ops)
{
  // TODO: pass bufferlist as arguments
  bufferlist bar1;
  bufferlist bar2;

  bar1.append(ceph::buffer::create(settings->block_size));
  fill_urandom(bar1.c_str(), settings->block_size);

  bar2.append(ceph::buffer::create(settings->block_size));
  fill_urandom(bar2.c_str(), settings->block_size);

  if (bar1.contents_equal(bar2))
    throw "Your RNG is not random";

  auto b = steady_clock::now();
  const auto stop = b + seconds(settings->secs);

  for (const auto &obj_name : obj_names)
  {
    ioctx.remove(obj_name);
  }

  while (b <= stop)
  {
    abort_if_signalled();
    if (ioctx.write(
        obj_names[rand() % 16],
        (ops.size() % 2) ? bar1 : bar2,
        settings->block_size,
        settings->block_size * (rand() % (settings->object_size / settings->block_size))
      ) < 0)
    {
      throw "Write error";
    }
    const auto b2 = steady_clock::now();
    ops.push_back(b2 - b);
    b = b2;
  }
}

static void do_bench(const unique_ptr<bench_settings> &settings, const vector<string> &names, IoCtx &ioctx)
{
  vector<steady_clock::duration> all_ops;

  if (settings->threads > 1)
  {
    vector<thread> threads;
    vector<vector<steady_clock::duration>> listofops;

    for (int i = 0; i < settings->threads; i++)
    {
      listofops.push_back(vector<steady_clock::duration>());
    }

    for (int i = 0; i < settings->threads; i++)
    {
      sigset_t new_set;
      sigset_t old_set;
      sigfillset(&new_set);
      int err;
      if ((err = pthread_sigmask(SIG_SETMASK, &new_set, &old_set)))
      {
        throw std::system_error(err, std::system_category(), "Failed to set thread sigmask");
      }

      threads.push_back(thread(_do_bench, ref(settings), vector<string>(names.begin()+i*16, names.begin()+i*16+16), ref(ioctx), ref(listofops[i])));

      if ((err = pthread_sigmask(SIG_SETMASK, &old_set, NULL)))
      {
        throw std::system_error(err, std::system_category(), "Failed to restore thread sigmask");
      }
    }

    for (auto &th : threads)
    {
      th.join();
    }

    for (const auto &res : listofops)
    {
      all_ops.insert(all_ops.end(), res.begin(), res.end());
    }
  }
  else
  {
    _do_bench(settings, names, ioctx, all_ops);
  }
  print_breakdown(all_ops, settings->threads);
}

static void _main(int argc, const char *argv[])
{
  const unique_ptr<bench_settings> settings(new bench_settings);

  // Default settings
  settings->secs = 10;
  settings->threads = 1;
  settings->block_size = 4096;
  settings->object_size = 4096 * 1024;

  int ai = 1;
  while (ai < argc)
  {
    if (argv[ai][0] == '-')
    {
      if (!strcmp(argv[ai], "-d"))
      {
        // duration
        ++ai;
        if (ai >= argc || sscanf(argv[ai], "%i", &settings->secs) != 1 ||
            settings->secs < 1)
          throw "Wrong duration";
      }
      else if (!strcmp(argv[ai], "-t"))
      {
        // threads
        ++ai;
        if (ai >= argc || sscanf(argv[ai], "%i", &settings->threads) != 1 ||
            settings->threads < 1)
          throw "Wrong thread number";
      }
      else if (!strcmp(argv[ai], "-b"))
      {
        // block size
        ++ai;
        if (ai >= argc || sscanf(argv[ai], "%i", (int*)&settings->block_size) != 1 ||
            settings->block_size < 1)
          throw "Wrong block size";
      }
      else if (!strcmp(argv[ai], "-o"))
      {
        // object size
        ++ai;
        if (ai >= argc || sscanf(argv[ai], "%i", (int*)&settings->object_size) != 1 ||
            settings->object_size < 1)
          throw "Wrong object size";
      }
    }
    else
    {
      if (settings->pool.empty())
        settings->pool = argv[ai];
      else if (settings->mode.empty())
        settings->mode = argv[ai];
      else if (settings->specific_bench_item.empty())
        settings->specific_bench_item = argv[ai];
    }
    ai++;
  }

  if (settings->object_size < settings->block_size)
  {
    throw "Block size must not be greater than object size";
  }

  if (settings->pool.empty() || settings->mode.empty())
  {
    cerr << "Usage: " << argv[0]
         << " [poolname] [mode=host|osd] <specific item name to test>" << endl;
    throw "Wrong cmdline";
  }

  Rados rados;
  int err;
  if ((err = rados.init("admin")) < 0)
  {
    cerr << "Failed to init: " << strerror(-err) << endl;
    throw "Failed to init";
  }

  if ((err = rados.conf_read_file("/etc/ceph/ceph.conf")) < 0)
  {
    cerr << "Failed to read conf file: " << strerror(-err) << endl;
    throw "Failed to read conf file";
  }

  if ((err = rados.conf_parse_argv(argc, argv)) < 0)
  {
    cerr << "Failed to parse argv: " << strerror(-err) << endl;
    throw "Failed to parse argv";
  }

  if ((err = rados.connect()) < 0)
  {
    cerr << "Failed to connect: " << strerror(-err) << endl;
    throw "Failed to connect";
  }

  // https://tracker.ceph.com/issues/24114
  this_thread::sleep_for(milliseconds(100));

  try
  {
    auto rados_utils = RadosUtils(&rados);

    if (rados_utils.get_pool_size(settings->pool) != 1)
      throw "It's required to have pool size 1";

    map<unsigned int, map<string, string>> osd2location;

    set<string> bench_items; // node1, node2 ||| osd.1, osd.2, osd.3

    for (const auto &osd : rados_utils.get_osds(settings->pool))
    {
      const auto &location = rados_utils.get_osd_location(osd);

      // TODO: do not fill this map if specific_bench_item specified
      osd2location[osd] = location;

      const auto &qwe = location.at(settings->mode);
      if (settings->specific_bench_item.empty() ||
          qwe == settings->specific_bench_item)
      {
        bench_items.insert(qwe);
      }
    }

    // benchitem -> [name1, name2] ||| i.e. "osd.2" => ["obj1", "obj2"]
    map<string, vector<string>> name2location;
    unsigned int cnt = 0;

    // for each bench_item find thread_count*16 names
    // store every name in name2location = [bench_item, names, description]
    cout << "Finding object names" << endl;
    const string prefix = "bench_";
    while (bench_items.size())
    {
      string name = prefix + to_string(++cnt);

      unsigned int osd = rados_utils.get_obj_acting_primary(name, settings->pool);

      const auto &location = osd2location.at(osd);
      const auto &bench_item = location.at(settings->mode);
      if (!bench_items.count(bench_item))
        continue;

      auto &names = name2location[bench_item];
      if (names.size() >= (unsigned)settings->threads*16)
      {
        bench_items.erase(bench_item);
        continue;
      }

      names.push_back(name);
    }

    IoCtx ioctx;

    if (rados.ioctx_create(settings->pool.c_str(), ioctx) < 0)
      throw "Failed to create ioctx";

    for (const auto &p : name2location)
    {
      const auto &bench_item = p.first;
      const auto &obj_names = p.second;
      cout << "Benching " << settings->mode << " " << bench_item << endl;
      do_bench(settings, obj_names, ioctx);
    }
  }
  catch (...)
  {
    rados.watch_flush();
    throw;
  }
  rados.watch_flush();

  //rados_ioctx_destroy(io);
  //rados_shutdown(cluster);
}

int main(int argc, const char *argv[])
{
  try
  {
    setup_signal_handlers();
    _main(argc, argv);
  }
  catch (const AbortException &msg)
  {
    cerr << "Test aborted" << endl;
    return 1;
  }
  catch (const char *msg)
  {
    cerr << "Unhandled exception: " << msg << endl;
    return 2;
  }
  cout << "Exiting successfully." << endl;
  return 0;
}
