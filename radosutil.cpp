#include <string>

#include <json/json.h>


#include "radosutil.h"

using namespace std;
using namespace librados;

RadosUtils::RadosUtils(Rados *rados_)
        : rados(rados_),                                               /* */
          json_reader(new Json::Reader(Json::Features::strictMode())), /* */
          json_writer(new Json::FastWriter())                          /* */
{}

// RadosUtils::~RadosUtils() {}

unsigned int RadosUtils::get_obj_acting_primary(const string &name,
                                                const string &pool) {

    Json::Value cmd(Json::objectValue);
    cmd["prefix"] = "osd map";
    cmd["object"] = name;
    cmd["pool"] = pool;

    auto &&location = do_mon_command(cmd);

    const auto &acting_primary = location["acting_primary"];
    if (!acting_primary.isNumeric())
        throw "Failed to get acting_primary";

    return acting_primary.asUInt();
}

// TODO:  std::map copying ? return unique_ptr ?
map <string, string> RadosUtils::get_osd_location(unsigned int osd) {
    Json::Value cmd(Json::objectValue);
    cmd["prefix"] = "osd find";
    cmd["id"] = osd;

    auto &&location = do_mon_command(cmd);
    const auto &crush = location["crush_location"];

    map <string, string> result;

    for (auto &&it = crush.begin(); it != crush.end(); ++it) {
        result[it.name()] = it->asString();
    }

    result["osd"] = "osd." + to_string(osd);

    return result;
}

// todo: std::set copying
set<unsigned int> RadosUtils::get_osds(const string &pool) {
    Json::Value cmd(Json::objectValue);
    cmd["prefix"] = "pg ls-by-pool";
    cmd["poolstr"] = pool;

    const auto &&pgs = do_mon_command(cmd);
    //cout << json_writer->write(pgs) << endl;

    set<unsigned int> osds;

    for (const auto &pg : pgs["pg_stats"]) {
        //cout << json_writer->write(pg) << endl;

        const auto &primary = pg["acting_primary"];
        //cout << primary << endl;
        if (!primary.isNumeric() || primary < 0)
            //throw "Failed to get acting_primary";
            continue;
        osds.insert(primary.asUInt());
    }

    return osds;
}

unsigned int RadosUtils::get_pool_size(const string &pool) {
    Json::Value cmd(Json::objectValue);
    cmd["prefix"] = "osd pool get";
    cmd["pool"] = pool;
    cmd["var"] = "size";
    const auto &&v = do_mon_command(cmd);
    return v["size"].asUInt();
}


unsigned int RadosUtils::set_pool_size_1(const string &pool) {
//    Json::Value cmd(Json::objectValue);
//    cmd["prefix"] = "osd pool get";
//    cmd["pool"] = pool;
//    cmd["var"] = "size";
    //string sizeval = "1";
    //cmd["val"] = sizeval;
//    const auto &&v = do_mon_command(cmd);
    int err;
    bufferlist outbl;
    string outs;
    bufferlist inbl;
    if ((err = rados->mon_command( "{\"prefix\": \"osd pool set\", \"pool\": \"" + pool +
                                   "\", \"var\": \"size\", \"val\":  \"1\"}"
            , inbl, &outbl, &outs)) < 0)
        throw MyRadosException(err, outs);

    return 0;
}

Json::Value RadosUtils::do_mon_command(Json::Value &cmd) {
    int err;
    bufferlist outbl;
    string outs;
    cmd["format"] = "json";
    bufferlist inbl;
    //cout << json_writer->write(cmd) << endl;
    if ((err = rados->mon_command(json_writer->write(cmd), inbl, &outbl, &outs)) <
        0)
        throw MyRadosException(err, outs);

    Json::Value root;
    if (!json_reader->parse(outbl.to_str(), root))
        throw "JSON parse error";

    //cout << json_writer->write(root) << endl;

    return root;
}
