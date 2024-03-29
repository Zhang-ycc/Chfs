#ifndef mr_protocol_h_
#define mr_protocol_h_

#include <string>
#include <vector>

#include "rpc.h"

using namespace std;

#define REDUCER_COUNT 4

enum mr_tasktype {
	NONE = 0, // this flag means no task needs to be performed at this point
	MAP,
	REDUCE
};

class mr_protocol {
public:
	typedef int status;
	enum xxstatus { OK, RPCERR, NOENT, IOERR };
	enum rpc_numbers {
		asktask = 0xa001,
		submittask,
	};

	struct AskTaskResponse {
		// Lab4: Your definition here.
        int taskType;
        int index;
        string filename;
        int file_num;
	};

    friend marshall &
    operator<<(marshall &m, const AskTaskResponse &res) {
        m << res.taskType << res.index << res.filename << res.file_num;
        return m;
    }

    friend unmarshall &
    operator>>(unmarshall &u, AskTaskResponse &res) {
        u >> res.taskType >> res.index >> res.filename >> res.file_num;
        return u;
    }

	struct AskTaskRequest {
		// Lab4: Your definition here.
	};

	struct SubmitTaskResponse {
		// Lab4: Your definition here.
	};

	struct SubmitTaskRequest {
		// Lab4: Your definition here.
	};

};

#endif

