const dotenv = require('dotenv');
dotenv.config();

const axios = require('axios');

const instance = axios.create({
    baseURL: process.env.API_URL || ''
});

const USADataRequest = async () => {
    const data = await instance
        .get('/data', { params: { drilldowns: "Nation", measures: "Population" } })
        .then(res => {
            if (res.status == 200) {
                return { data: res.data.data }
            } else {
                return { 'data': [] } 
            }
        })
        .catch(err => {
            console.log(err)
            return { 'data': [] } 
        });

    return data;
}

module.exports = { USADataRequest }