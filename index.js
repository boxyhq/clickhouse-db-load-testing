const { ClickHouse } = require('clickhouse');

const clickhouse = new ClickHouse();

var options = {};
var testStartTime, testEndTime, totalRowsFetched = 0, requestCount = 0, counter = 0, lastFailedCount = 0;
function sleep(ms) {
    return new Promise(resolve => {
        setTimeout(resolve, ms)
    })
}

let reqInfo = {}, report = {}, incrementFactor = 0.2;

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
        options.type = "random";
        options.reqPerBatch = 10;
        options.numOfBatches = 1;
}

const sendRequests = async () => {
    try {
        let reqDirectory = require(`./query/${reqInfo.file}`), timeStamps;
        if(reqDirectory['getInitData']) {
            timeStamps = await reqDirectory['getInitData'](clickhouse);
        }
        let reqFactory = reqDirectory[reqInfo.func];
        let responses = 0, totalFailed = 0;
        if (options.type === "onetime") {
            requestCount = options.numOfBatches * options.reqPerBatch;
            testStartTime = +new Date();
            for (let i = 0; i < options.numOfBatches; i++) {
                //Create batch
                for (let j = 0; j < options.reqPerBatch; j++) {
                    let queryInfo = reqFactory(timeStamps);
                    report[(i + 1) * (j + 1)] = { startTime: +new Date(), query: queryInfo.query, where: Object.keys(queryInfo.where).join(' ') };
                    sendQuery(queryInfo.query, (i + 1) * (j + 1)).then((res) => {
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
            do {
                requestCount = options.numOfBatches * options.reqPerBatch;
                testStartTime = +new Date();
                for (let i = 0; i < options.numOfBatches; i++) {
                    //Create batch
                    for (let j = 0; j < options.reqPerBatch; j++) {
                        let queryInfo = reqFactory();
                        report[counter] = { startTime: +new Date(), query: queryInfo.query, where: Object.keys(queryInfo.where).join(' ') };
                        sendQuery(queryInfo.query, counter).then((res) => {
                            // report[queries.indexOf(query)].endTime = new Date();
                            responses++;
                            totalRowsFetched += res.rows.length;
                            // console.log("Response Received");
                            // console.log(`Record count => ${res.rows.length}`);
                            // console.log(`Total Failed: ${totalFailed}`);
                        }).catch(err => {
                            // console.log(err);
                            responses++;
                            totalFailed++;
                            // console.log("Error Response Received");
                            // console.log(`Total Failed: ${totalFailed}`);
                        });
                        counter++;
                        console.clear();
                        console.log(`
                        [Req Per Batch] => ${options.reqPerBatch}
                        [Total Failed]  => ${totalFailed}
                        `);
                    };
                    await sleep(500);
                }
                //await sleep(2000);
                console.log(`[Recalibrating Options]`);
                options.reqPerBatch = getNewOptions(totalFailed);
            } while (true);
        }
    } catch (ex) {
        console.error(ex);
        process.exit(1);
    }
};

const getNewOptions = (totalFailed) => {
    let latestFailed = totalFailed;
    let diff = latestFailed - lastFailedCount;
    lastFailedCount = latestFailed;
    let percentageFailed, reqPerBatch;
    if (diff === 0) {        
        reqPerBatch = Math.floor(options.reqPerBatch * (1 + incrementFactor));
        incrementFactor += 0.1;
    } else {
        incrementFactor = 0.1;
        percentageFailed = diff < options.reqPerBatch ? diff / options.reqPerBatch : 0.5;
        reqPerBatch = Math.floor(options.reqPerBatch * (1 - percentageFailed));
    }
    reqPerBatch = reqPerBatch <= 0 ? 10 : reqPerBatch;
    console.log(`[Old Req Per Batch ${options.reqPerBatch}] => [New Req Per Batch ${reqPerBatch}]`);
    return reqPerBatch;
}

const randomIntFromInterval = (min, max) => { // min and max included 
    return Math.floor(Math.random() * (max - min + 1) + min)
}

const sendQuery = async (query, index) => {
    // report.push({
    //     index: index,
    //     query: query,
    //     startTime: new Date()
    // });
    return new Promise((resolve, reject) => {
        try {
            let res = {
                index: index
            };
            clickhouse.query(query).exec(function (err, rows) {
                if (err) {
                    report[index] = { ...report[index],rows: 0,  err: err.message.replace(',', ' '), duration: (+new Date() - report[index].startTime) / 1000 }
                    reject(err);
                } else {
                    report[index] = { ...report[index], rows: rows.length, err: '', duration: (+new Date() - report[index].startTime) / 1000 }
                    resolve({
                        index: res.index,
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
    console.log(`[${responses}/${requestCount}]`);
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
    if (responses == options.numOfBatches * options.reqPerBatch) {
        console.log(`Total: ${(options.reqPerBatch * options.numOfBatches)}`);
        console.log(`Passed: ${(options.reqPerBatch * options.numOfBatches) - totalFailed}`);
        console.log(`% Failed: ${(totalFailed * 100) / (options.reqPerBatch * options.numOfBatches)}`);
        console.log(`Total row fetched: ${totalRowsFetched}`);
        let seconds = (testEndTime - testStartTime) / 1000;
        console.log(`Requests per seconds: ${(requestCount - totalFailed) / seconds}`);
        console.log(`Rows fetched per seconds: ${totalRowsFetched / seconds}`);
        // console.table(report);
        saveReportToCSV(report);
        var player = require('play-sound')(opts = {})
        // player.play('./foo.mp3', function(err){
        //     if (err) throw err
        // })
        //process.exit(1);
    }
}
const { writeFile } = require('fs').promises;
const { v4: uuidv4 } = require('uuid');

const saveReportToCSV = async () => {
    const data = [];
    let keys = Object.keys(report);
    let outputFileName = `./reports/${uuidv4()}.csv`;
    for (let i = 0; i < keys.length; i++) {
        data.push(report[keys[i]]);
    }
    const CSV = arrayToCSV(data);
    await writeCSV(outputFileName, CSV);
    console.log(`Successfully converted ${outputFileName}!`);
}

function arrayToCSV (data) {
    csv = data.map(row => Object.values(row));
    csv.unshift(Object.keys(data[0]));
    return `"${csv.join('"\n"').replace(/,/g, '","')}"`;
}

async function writeCSV(fileName, data) {
    try {
        await writeFile(fileName, data, 'utf8');
    } catch (err) {
        console.log(err);
        process.exit(1);
    }
}