const _ = require('lodash');
const fs = require("fs");
const Papa = require("papaparse");
const v8 = require('node:v8');

let totalRepeated = 0;


const init = () => {

    console.info('Init parse...');
    // Es recomendado al menos tener configurado 8 GB => export NODE_OPTIONS="--max-old-space-size=8192"
    console.info(`Total memory: ${v8.getHeapStatistics().heap_size_limit / (1024 * 1024)} MB`); 

    const config = { header: true, skipEmptyLines: 'greedy', fastMode: true };
    const file = fs.createReadStream('./merged.csv');
    
    console.info(`Reading file: ${file.path}`);
    
    let data = [];
    
    Papa.parse(file, {
        ...config,
        step: (results, parser) => {
            if (results.errors?.length)
                console.error('Row errors', results.errors);
            data.push(results.data);
            // if (data.length === 100) {
            //     parser.pause();
            //     processCSV(data);
            //     process.exit();
            //     //parser.resume();
            // }
    
        },
        complete: function () {
            processCSV(data);
            console.info('Parsing complete');
            process.exit();
        }
    })
};


const processCSV = (rows) => {
    
    const totalRows = rows.length;
    console.info(`Total rows: ${totalRows}`);

    const result = _(rows)
        .groupBy('doc_number')
        //.reject((o) => o.length <= 2) -> Esto si queremos hacer pruebas dejando los que estan mas de 2 veces repetidos.
        //.take() -> Sirve para tomar uno o varios de ejemplo para no procesar todo.
        .map((array) => {
            if (array.length > 1) { // Los que estan duplicados hacemos un merge con todos los que existan.
                //console.debug(array)
                totalRepeated++;
                return _.mergeWith(...array, customizer); // Merge de los duplicados.
            }
            //console.debug(array.length)
            return trimObject(array[0]); // Hacemos un trim de todas las properties del objeto, ya que existen valores con espacios. -> "PEDRO  "
        })
        .value();

    console.info(`Total repeated: ${totalRepeated}`);
    console.info(`Total processed: ${result.length}`);

    console.info('Parsing data to CSV');
    const csv = Papa.unparse(result, { delimiter: ';' });
    
    console.info('Writing output file: output.csv');
    fs.writeFileSync('output.csv', csv);

};


/**
 * Recorremos todas las properties del objeto y le sacamos los espacios.
 * 
 * @param {*} obj 
 * @returns Devolvemos el objeto sin espacios
 */
const trimObject = (obj) => {
    return _.each(obj, (v,k) => obj[k] = v.trim());
};


/**
 * 
 * @param {*} objValue Valor property objeto destino
 * @param {*} srcValue Valor property objeto fuente
 * @param {*} key      Nombre de la property
 * @param {*} object   Objeto destino
 * @param {*} source   Objeto fuente
 * @param {*} stack 
 * @returns 
 */
const customizer = (objValue, srcValue, key, object, source, stack) => {

    const trimObjValue = _.trim(objValue);
    const trimSrcValue = _.trim(srcValue);
    const value = _.isEmpty(trimObjValue) ? trimSrcValue : trimObjValue;

    // console.debug(`Final value: ${value}`);

    // console.debug(`objValue: ${objValue} is empty: ${_.isEmpty(objValue)}`);
    // console.debug(`srcValue: ${srcValue} is empty: ${_.isEmpty(srcValue)}`);
    // console.debug(`key: ${key}`);
    // console.debug(object);
    // console.debug(source);
    // console.debug(stack);

    return value;
};

init();
