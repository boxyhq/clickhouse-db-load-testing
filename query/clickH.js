const getRandomQuery = (timeStamps) => {
    let filtersValues = {
        tenentId: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        actor: ['Deepak', 'Vishal', 'Kiran', 'Utkarsh', 'Aswin', 'Sama'],
        actor_type: ['user', 'admin', 'dev'],
        group: ['dev', 'business'],
        where_type: ['ip', 'city'],
        where: ['127.0.0.1', '192.168.1.1', 'Pune', 'Chennei', 'London'],
        target: ['user.login', 'user.profile', 'dashboard.edit', 'dashboard.view', 'user.logout'],
        target_id: ['10', '20', '30', '40'],
        action: ['login', 'logout', 'view', 'scroll', 'click'],
        action_type: ['U', 'V', 'C', 'S', 'L']
    }
    let where = {};
    const getRandomValue = (collection) => {
        return collection[randomIntFromInterval(0, collection.length - 1)];
    }
    const randomIntFromInterval = (min, max) => { // min and max included 
        return Math.floor(Math.random() * (max - min + 1) + min)
    }
    let numberOfFilters = randomIntFromInterval(1, Object.keys(filtersValues).length - 1);
    let query = `SELECT * FROM hermes.auditlogs`;
    if (numberOfFilters > 0) {
        let filters = Object.keys(filtersValues);
        query += ' WHERE';
        for (let i = 0; i < numberOfFilters; i++) {
            let column = getRandomValue(filters);
            let val = getRandomValue(filtersValues[column]);
            where[column] = val;
            filters = filters.filter(e => e !== column);
            query += ` (${column} = ${typeof val === 'string' ? "'" : ""}${val}${typeof val === 'string' ? "'" : ''})`;
            if (i <= numberOfFilters - 2) {
                query += ' AND';
            }
        }
    }
    if(timeStamps) {
        if(Object.keys(timeStamps).length >= 2) {
            let first = randomIntFromInterval(+new Date(timeStamps.start), +new Date(timeStamps.end));
            query+= ` AND timestamp > toDate(${first})`
        }
    }
    query += ' LIMIT 100';
    return {query: query, where: where};
}

const getInitData = async (connection) => {
    try {
        return new Promise((resolve, reject) => {
            let data = {};
            connection.query(`SELECT DISTINCT (timestamp) FROM hermes.auditlogs ORDER BY timestamp LIMIT 1;`).exec((er, row) => {
                if(!er) {
                    console.log(row[0].timestamp);
                    data.start = row[0].timestamp;
                    connection.query(`SELECT DISTINCT (timestamp) FROM hermes.auditlogs ORDER BY timestamp DESC LIMIT 1;`).exec((_er, _row) => {
                        if(!_er) {
                            console.log(_row[0].timestamp);
                            data.end = new Date(_row[0].timestamp);
                            resolve(data);
                        } else {
                            resolve({});
                        }
                    });
                } else {
                    resolve({});
                }
            });
        });
    } catch(ex) {
        console.log(ex);
    }
}

module.exports = {
    query: getRandomQuery,
    getInitData: getInitData
};