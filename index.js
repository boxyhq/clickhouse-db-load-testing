const { ClickHouse } = require('clickhouse');

const clickhouse = new ClickHouse();

var options = {}, queries = [];
var testStartTime, testEndTime, totalRowsFetched = 0, requestCount = 0;
function sleep(ms) {
    return new Promise(resolve => {
        setTimeout(resolve, ms)
    })
}

let reqInfo = {};

//Options
let reqFile = process.argv.length >= 3 ? process.argv[2] : 'hermes.ingest';
if (!reqFile) {
    console.warn("Request file and method required!");
    process.exit(1);
} else {
    let parts = reqFile.split('.');
    if (parts.length === 1) {
        console.warn("Request file and method required!");
        process.exit(1);
    } else {
        reqInfo.file = parts[0];
        reqInfo.func = parts[1];
    }
}

let type = process.argv.length >= 4 ? process.argv[3] : "onetime";

switch (type) {
    case "onetime":
        options.type = type;
        let reqPerBatch = process.argv.length >= 5 ? parseInt(process.argv[4]) : 10;
        options.reqPerBatch = reqPerBatch;
        let numOfBatches = process.argv.length >= 6 ? parseInt(process.argv[5]) : 2;
        options.numOfBatches = numOfBatches;

        break;
    case "incr":
        options.type = type;
        let start = process.argv.length >= 5 ? parseInt(process.argv[4]) : 10;
        options.start = start;
        let increment = process.argv.length >= 6 ? parseInt(process.argv[5]) : 2;
        options.increment = increment;
        let end = process.argv.length >= 7 ? parseInt(process.argv[6]) : 20;
        options.end = end;
        break;
    default:
        console.warn("Invalid benchmarking type", type);
        process.exit(1);
}

const sendRequests = async () => {
    try {
        let reqFactory = require(`./query/${reqInfo.file}`)[reqInfo.func];
        let responses = 0, totalFailed = 0;
        if (options.type === "onetime") {
            requestCount = options.numOfBatches * options.reqPerBatch;
            testStartTime = +new Date();
            for (let i = 0; i < options.numOfBatches; i++) {
                //Create batch
                for (let j = 0; j < options.reqPerBatch; j++) {
                    let query;
                    do { query = reqFactory() } while (queries.indexOf(query) !== -1);
                    sendQuery(query, (i + 1) * (j + 1)).then((res) => {
                        // report[queries.indexOf(query)].endTime = new Date();
                        responses = printSuccessLog(responses, requestCount, res, totalFailed);
                        printConsolidatedInfo(responses, totalFailed);
                    }).catch(err => {
                        ({ responses, totalFailed } = printFailureLog(err, responses, totalFailed));
                        printConsolidatedInfo(responses, totalFailed);
                    });
                };
                await sleep(500);
            }
        } else if (options.type === "incr") {

        } else {

        }
    } catch (ex) {
        console.error(ex);
        process.exit(1);
    }
};

const sendQuery = async (query, index) => {
    // report.push({
    //     index: index,
    //     query: query,
    //     startTime: new Date()
    // });
    return new Promise((resolve, reject) => {
        try {
            clickhouse.query(query).exec(function (err, rows) {
                if (err) {
                    reject(err);
                } else {
                    resolve({
                        success: true,
                        rows: rows
                    });
                }
            });
        } catch (ex) {
            reject(ex);
        }
    });
}


//Initiate the program
sendRequests();


function printFailureLog(err, responses, totalFailed) {
    console.log(err);
    responses++;
    totalFailed++;
    console.log("Error Response Received");
    console.log(`[${responses}/${options.numOfBatches}]`);
    console.log(`Total Failed: ${totalFailed}`);
    return { responses, totalFailed };
}

function printSuccessLog(responses, requestCount, res, totalFailed) {
    responses++;
    totalRowsFetched += res.rows.length;
    console.log("Response Received");
    console.log(`[${responses}/${requestCount}] Record count => ${res.rows.length}`);
    console.log(`Total Failed: ${totalFailed}`);
    return responses;
}

function printConsolidatedInfo(responses, totalFailed) {
    testEndTime = +new Date();
    if (responses == options.numOfBatches) {
        console.log(`Total: ${(options.reqPerBatch * options.numOfBatches)}`);
        console.log(`Passed: ${(options.reqPerBatch * options.numOfBatches) - totalFailed}`);
        console.log(`% Failed: ${(totalFailed * 100) / (options.reqPerBatch * options.numOfBatches)}`);
        console.log(`Total row fetched: ${totalRowsFetched}`);
        let seconds = (testEndTime - testStartTime) / 1000;
        console.log(`Requests per seconds: ${requestCount / seconds}`);
        console.log(`Rows fetched per seconds: ${totalRowsFetched / seconds}`);
        process.exit(1);
    }
}
/*
 * first arg => req file/folder
 * type of test =>  one time(onetime), incremental(incr)
 *                  one time => 10 * 10 => 100
 *                  incremental => 10 => 10 + 5 => 50
*/