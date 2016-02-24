// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/01/14 13:45:46

#include <fstream>
#include <bthread_unstable.h>
#include <gflags/gflags.h>
#include <base/containers/flat_map.h>
#include <base/logging.h>
#include <base/comlog_sink.h>
#include <bthread.h>
#include <baidu/rpc/controller.h>
#include <baidu/rpc/server.h>
#include <raft/util.h>
#include <raft/storage.h>
#include "state_machine.h"
#include "cli_service.h"
#include "atomic.pb.h"

DEFINE_int32(map_capacity, 1024, "Initial capicity of value map");
DEFINE_string(ip_and_port, "0.0.0.0:8000", "server listen address");
DEFINE_string(name, "test", "Name of the raft group");
DEFINE_string(peers, "", "cluster peer set");
DEFINE_int32(snapshot_interval, 30, "Interval between each snapshot");
DEFINE_int32(election_timeout_ms, 5000, 
            "Start election after no message received from leader in such time");

namespace example {

struct AtomicClosure : public raft::Closure {
    void Run() {
        if (_err_code) {
            cntl->SetFailed(_err_code, "%s", _err_text.c_str());
        }
        done->Run();
        delete this;
    }
    baidu::rpc::Controller* cntl;
    google::protobuf::Message* response;
    google::protobuf::Closure* done;
};

class Atomic : public CommonStateMachine {
public:
    Atomic(const raft::GroupId& group_id, const raft::PeerId& peer_id) 
        : CommonStateMachine(group_id, peer_id) {
            CHECK_EQ(0, _value_map.init(FLAGS_map_capacity));
    }

    virtual ~Atomic() {}

    // FSM method
    void on_apply(const int64_t /*index*/,
                  const raft::Task& task) {
        raft::Closure* done = task.done;
        baidu::rpc::ClosureGuard done_guard(done);
        base::IOBufAsZeroCopyInputStream wrapper(*task.data);
        CompareExchangeRequest req;
        if (!req.ParseFromZeroCopyStream(&wrapper)) {
            if (done) {
                done->set_error(baidu::rpc::EREQUEST,
                                "Fail to parse buffer");
            }
            LOG(INFO) << "Fail to parse CompareExchangeRequest";
            return;
        }
        const int64_t id = req.id();
        int64_t& cur_val = _value_map[id];
        CompareExchangeResponse* res = NULL;
        if (done) {
            res = (CompareExchangeResponse*)((AtomicClosure*)done)->response;
            res->set_old_value(cur_val);
        }
        if (cur_val == req.expected_value()) {
            cur_val = req.new_value();
            if (res) { res->set_success(true); }

        } else {
            if (res) { res->set_success(false); }
        }
        if (done) {
            return raft::run_closure_in_bthread(done_guard.release());
        }
    }

    void on_apply_in_batch(const int64_t first_index, 
                           const raft::Task tasks[],
                           size_t size) {
        for (size_t i = 0; i < size; ++i) {
            on_apply(first_index + i, tasks[i]);
        }
        bthread_flush();
    }

    void on_shutdown() {
        // FIXME: should be more elegant
        exit(0);
    }

    void on_snapshot_save(raft::SnapshotWriter* writer, raft::Closure* done) {
        SnapshotClosure* sc = new SnapshotClosure;
        sc->values.reserve(_value_map.size());
        sc->writer = writer;
        sc->done = done;
        for (ValueMap::const_iterator 
                it = _value_map.begin(); it != _value_map.end(); ++it) {
            sc->values.push_back(std::make_pair(it->first, it->second));
        }
        bthread_t tid;
        if (bthread_start_background(&tid, NULL, save_snaphsot, sc) != 0) {
            PLOG(ERROR) << "Fail to start bthread";
            save_snaphsot(sc);
        }
    }

    int on_snapshot_load(raft::SnapshotReader* reader) {
        // TODO: verify snapshot
        _value_map.clear();
        std::string snapshot_path = 
                raft::fileuri2path(reader->get_uri(base::EndPoint()));
        snapshot_path.append("/data");
        std::ifstream is(snapshot_path.c_str());
        int64_t id = 0;
        int64_t value = 0;
        while (is >> id >> value) {
            _value_map[id] = value;
        }
        return 0;
    }

    // Acutally we don't care now
    void on_leader_start() {}
    void on_leader_stop() {}

    void apply(base::IOBuf *iobuf, raft::Closure* done) {
        raft::Task task;
        task.data = iobuf;
        task.done = done;
        return _node.apply(task);
    }

private:

    static void* save_snaphsot(void* arg) {
        SnapshotClosure* sc = (SnapshotClosure*)arg;
        std::unique_ptr<SnapshotClosure> sc_guard(sc);
        baidu::rpc::ClosureGuard done_guard(sc->done);
        std::string snapshot_path = 
                raft::fileuri2path(sc->writer->get_uri(base::EndPoint()));
        snapshot_path.append("/data");
        std::ofstream os(snapshot_path.c_str());
        for (size_t i = 0; i < sc->values.size(); ++i) {
            os << sc->values[i].first << ' ' << sc->values[i].second << '\n';
        }
        return NULL;
    }

    typedef base::FlatMap<int64_t, int64_t> ValueMap;

    struct SnapshotClosure {
        std::vector<std::pair<int64_t, int64_t> > values;
        raft::SnapshotWriter* writer;
        raft::Closure* done;
    };
    // TODO: To support the read-only load, _value_map should be COW or doubly
    // buffered
    ValueMap _value_map;
};

class AtomicServiceImpl : public AtomicService {
public:

    AtomicServiceImpl() : _atomic(NULL) {}

    // rpc method
    virtual void compare_exchange(::google::protobuf::RpcController* controller,
                       const ::example::CompareExchangeRequest* request,
                       ::example::CompareExchangeResponse* response,
                       ::google::protobuf::Closure* done) {
        baidu::rpc::ClosureGuard done_guard(done);
        baidu::rpc::Controller* cntl = (baidu::rpc::Controller*)controller;
        Atomic* atomic = _atomic.load(boost::memory_order_consume);
        if (atomic  == NULL) {
            cntl->SetFailed(baidu::rpc::EINTERNAL, "Not already initialized");
            return;
        }
        base::IOBuf data;
        base::IOBufAsZeroCopyOutputStream wrapper(&data);
        if (!request->SerializeToZeroCopyStream(&wrapper)) {
            cntl->SetFailed(baidu::rpc::EREQUEST, "Fail to serialize request");
            return;
        }
        AtomicClosure* c = new AtomicClosure;
        c->cntl = cntl;
        c->response = response;
        c->done = done_guard.release();
        return atomic->apply(&data, c);
    }

    void set_atomic(Atomic* atomic) {
        _atomic.store(atomic, boost::memory_order_release);
    }

private:
    boost::atomic<Atomic*> _atomic;
};

}  // namespace example

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    // [ Setup from ComlogSinkOptions ]
    logging::ComlogSinkOptions options;
    //options.async = true;
    options.process_name = "atomic_server";
    options.print_vlog_as_warning = false;
    options.split_type = logging::COMLOG_SPLIT_SIZECUT;
    if (logging::ComlogSink::GetInstance()->Setup(&options) != 0) {
        LOG(ERROR) << "Fail to setup comlog";
        return -1;
    }
    logging::SetLogSink(logging::ComlogSink::GetInstance());

    // add service
    baidu::rpc::Server server;
    example::AtomicServiceImpl service;
    if (0 != server.AddService(&service, 
                baidu::rpc::SERVER_DOESNT_OWN_SERVICE)) {
        LOG(FATAL) << "Fail to AddService";
        return -1;
    }
    example::CliServiceImpl cli_service_impl(NULL);
    if (0 != server.AddService(&cli_service_impl, 
                baidu::rpc::SERVER_DOESNT_OWN_SERVICE)) {
        LOG(FATAL) << "Fail to AddService";
        return -1;
    }

    // init raft and server
    baidu::rpc::ServerOptions server_options;
    if (0 != raft::start_raft(FLAGS_ip_and_port.c_str(), &server, &server_options)) {
        LOG(FATAL) << "Fail to init raft";
        return -1;
    }

    // init peers
    std::vector<raft::PeerId> peers;
    const char* the_string_to_split = FLAGS_peers.c_str();
    for (base::StringSplitter s(the_string_to_split, ','); s; ++s) {
        raft::PeerId peer(std::string(s.field(), s.length()));
        peers.push_back(peer);
    }

    base::EndPoint addr;
    base::str2endpoint(FLAGS_ip_and_port.c_str(), &addr);
    if (base::IP_ANY == addr.ip) {
        addr.ip = base::get_host_ip();
    }
    // init counter
    example::Atomic* atomic = new example::Atomic(FLAGS_name, raft::PeerId(addr, 0));
    raft::NodeOptions node_options;
    node_options.election_timeout = FLAGS_election_timeout_ms;
    node_options.fsm = atomic;
    node_options.conf = raft::Configuration(peers); // bootstrap need
    node_options.snapshot_interval = FLAGS_snapshot_interval;
    // TODO: should be options
    node_options.log_uri = "file://./data/log";
    node_options.stable_uri = "file://./data/stable";
    node_options.snapshot_uri = "file://./data/snapshot";

    if (0 != atomic->init(node_options)) {
        LOG(FATAL) << "Fail to init node";
        return -1;
    }
    LOG(INFO) << "init Node success";

    service.set_atomic(atomic);
    cli_service_impl.set_state_machine(atomic);
    LOG(INFO) << "Wait until server stopped";
    server.Join();
    LOG(INFO) << "AtomicServer is going to quit";
    raise(SIGKILL);
    raft::stop_raft(FLAGS_ip_and_port.c_str(), NULL);

    return 0;
}