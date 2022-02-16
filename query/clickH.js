const getRandomQuery = () => {
    let filtersValues = {
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
            query += ` (${column} = '${val}')`;
            if(i <= numberOfFilters - 2) {
                query += ' AND';
            }
        }
    }
    query += ' LIMIT 100';
    return {query: query, where: where};
}
module.exports = {
    query: getRandomQuery
};