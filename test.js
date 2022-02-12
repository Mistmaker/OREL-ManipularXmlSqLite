const fs = require('fs');
const sqlite3 = require('sqlite3').verbose();
var parser = require('fast-xml-parser');

const archivos = process.argv[2];

let db = new sqlite3.Database('sri.db', (err) => {
    if (err) {
        console.error(err.message);
    }
    // console.log('Connected to the sri database.');
});

var files = [
    'JUANABUSTILLOS/AT-012020_JUANA_UNIFICADO.xml',
    'JUANABUSTILLOS/AT-032020JuanaB.xml',
    'JUANABUSTILLOS/AT-042020-JUANA.xml',
    'JUANABUSTILLOS/AT-052020.xml',
    'JUANABUSTILLOS/AT-062020_JUANA.xml',
    'JUANABUSTILLOS/AT-072020-JB.xml',
    'JUANABUSTILLOS/AT-082020.xml',
    'JUANABUSTILLOS/AT-092020.xml',
    'JUANABUSTILLOS/AT-102020.xml',
    'JUANABUSTILLOS/AT-112020.xml',
    'JUANABUSTILLOS/AT-122020.xml',
    'CARLOSAVILES/A.COLIBRI2020_AT012020.xml',
    'CARLOSAVILES/A.COLIBRI2020_AT022020.xml',
    'CARLOSAVILES/A.COLIBRI2020_AT032020.xml',
    'CARLOSAVILES/A.COLIBRI2020_AT052020.xml',
    'CARLOSAVILES/A.COLIBRI2020_AT062020.xml',
    'CARLOSAVILES/A.COLIBRI2020_AT072020.xml',
    'CARLOSAVILES/A.COLIBRI2020_AT082020.xml',
    'CARLOSAVILES/A.COLIBRI2020_AT092020.xml',
    'CARLOSAVILES/A.COLIBRI2020_AT112020.xml',
    'CARLOSAVILES/A.COLIBRI2020_AT122020.xml',
    'CARLOSAVILES/AT-042020CARLOSAVILES.xml',
    'GENESISBUSTILLOS/AT-012020_gene.xml',
    'GENESISBUSTILLOS/AT-022020GENESSI.xml',
    'GENESISBUSTILLOS/AT-032020.xml',
    'GENESISBUSTILLOS/AT-042020-GENESIS.xml',
    'GENESISBUSTILLOS/AT-052020.xml',
    'GENESISBUSTILLOS/AT-062020_GENESIS_GALLARDO.xml',
    'GENESISBUSTILLOS/AT-072020.xml',
    'GENESISBUSTILLOS/AT-082020_genesis.xml',
    'GENESISBUSTILLOS/AT-092020.xml',
    'GENESISBUSTILLOS/AT-102020.xml',
    'GENESISBUSTILLOS/AT-112020.xml',
    'GENESISBUSTILLOS/AT-122020.xml',
];

files = archivos.split(',');

const sleep = ms => {
    return new Promise(resolve => setTimeout(resolve, ms))
}

async function cargarAnexo(ruta) {
    return new Promise((resolve, reject) => {
        fs.readFile(ruta, 'utf8', (err, data) => {
            if (err) throw err;
            xmlData = data;

            try {
                var jsonObj = parser.parse(xmlData, { parseTrueNumberOnly: true }, true);

                const ruc = jsonObj.iva.IdInformante.toString();
                const razonSocial = jsonObj.iva.razonSocial;
                const Anio = jsonObj.iva.Anio;
                const Mes = jsonObj.iva.Mes;

                db.run(`DELETE FROM compras where idinformante = '${ruc}' AND anio = '${Anio}' AND mes = '${Mes}'`);
                db.run(`DELETE FROM retenciones where informante = '${ruc}' AND anio = '${Anio}' AND mes = '${Mes}'`);
                db.run(`DELETE FROM ventas where informante = '${ruc}' AND anio = '${Anio}' AND mes = '${Mes}'`);

                if (jsonObj.iva.compras) {
                    for (var compras of Object.entries(jsonObj.iva.compras.detalleCompras)) {
                        var compra = compras[1];
                        const codigo = compra.tpIdProv + compra.tipoComprobante + compra.establecimiento + compra.puntoEmision + compra.secuencial;
                        compra.idProv = compra.idProv.toString();
                        db.serialize(function () {
                            var stmt = db.prepare("INSERT INTO compras VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
                            stmt.run([null, ruc, razonSocial, Anio, Mes, compra.codSustento, compra.tpIdProv, compra.idProv, compra.tipoComprobante, null, null, compra.parteRel, compra.fechaRegistro, compra.establecimiento, compra.puntoEmision, compra.secuencial, compra.fechaEmision, compra.autorizacion, compra.baseNoGraIva, compra.baseImponible, compra.baseImpGrav, compra.baseImpExe, compra.montoIce, compra.montoIva, compra.valRetBien10, compra.valRetServ20, compra.valorRetBienes, compra.valRetServ50, compra.valorRetServicios, compra.valRetServ100, compra.totbasesImpReemb, null, codigo, null, null]);
                            stmt.finalize();
                        });
                        if (compra.air) { // tiene retención
                            for (const retenciones of Object.entries(compra.air)) {
                                var retencion = retenciones[1];
                                db.serialize(function () {
                                    var stmt = db.prepare("INSERT INTO retenciones VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
                                    stmt.run([null, compra.secuencial, compra.estabRetencion1, compra.ptoEmiRetencion1, compra.secRetencion1, compra.autRetencion1, compra.fechaEmiRet1, retencion.codRetAir, retencion.baseImpAir, retencion.porcentajeAir, retencion.valRetAir, ruc, Anio, Mes, null, codigo]);
                                    stmt.finalize();
                                });
                            }
                        }
                    }
                }

                if (jsonObj.iva.ventas) {
                    for (var ventas of Object.entries(jsonObj.iva.ventas.detalleVentas)) {
                        var venta = ventas[1];
                        venta.idCliente = venta.idCliente.toString();
                        venta.idCliente = venta.idCliente.length === 12 || venta.idCliente.length === 9 ? '0' + venta.idCliente : venta.idCliente;
                        db.serialize(function () {
                            var stmt = db.prepare("INSERT INTO ventas VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
                            stmt.run([null, ruc, razonSocial, Anio, Mes, venta.tpIdCliente, venta.idCliente, venta.parteRelVentas, venta.tipoComprobante, venta.tipoEmision, venta.numeroComprobantes, venta.baseNoGraIva, venta.baseImponible, venta.baseImpGrav, venta.montoIva, venta.montoIce, venta.valorRetIva, venta.valorRetRenta, null]);
                            stmt.finalize();
                        });
                    }
                }
                resolve(true);
            } catch (error) {
                return reject('error =>', error.message)
            }
        });


    });
}

const IniciarCarga = async _ => {
    for (const file of files) {
        const resp = await cargarAnexo(file);
    }
    console.log('Archivos cargados con exito');
    db.close();
}


IniciarCarga();