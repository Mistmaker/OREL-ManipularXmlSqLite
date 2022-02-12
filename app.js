const fs = require('fs');
const sqlite3 = require('sqlite3').verbose();
var parser = require('fast-xml-parser');
var http = require('http');

const archivos = process.argv[2];

let db = new sqlite3.Database('sri.db', (err) => {
    if (err) {
        console.error(err.message);
    }
    // console.log('Connected to the sri database.');
});

files = archivos.split(',');

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
                    for (var ventas of Object.entries(jsonObj.iva.ventas)) {
                        var venta = ventas[1];
                        if (venta.length) {
                            for (var v of venta) {
                                v.idCliente = v.idCliente.toString();
                                v.idCliente = v.idCliente.length === 12 || v.idCliente.length === 9 ? '0' + v.idCliente : v.idCliente;
                                db.serialize(function () {
                                    var stmt = db.prepare("INSERT INTO ventas VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
                                    stmt.run([null, ruc, razonSocial, Anio, Mes, v.tpIdCliente, v.idCliente, v.parteRelVentas, v.tipoComprobante, v.tipoEmision, v.numeroComprobantes, v.baseNoGraIva, v.baseImponible, v.baseImpGrav, v.montoIva, v.montoIce, v.valorRetIva, v.valorRetRenta, null]);
                                    stmt.finalize();
                                });
                            }
                        } else {
                            venta.idCliente = venta.idCliente.toString();
                            venta.idCliente = venta.idCliente.length === 12 || venta.idCliente.length === 9 ? '0' + venta.idCliente : venta.idCliente;
                            db.serialize(function () {
                                var stmt = db.prepare("INSERT INTO ventas VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
                                stmt.run([null, ruc, razonSocial, Anio, Mes, venta.tpIdCliente, venta.idCliente, venta.parteRelVentas, venta.tipoComprobante, venta.tipoEmision, venta.numeroComprobantes, venta.baseNoGraIva, venta.baseImponible, venta.baseImpGrav, venta.montoIva, venta.montoIce, venta.valorRetIva, venta.valorRetRenta, null]);
                                stmt.finalize();
                            });
                        }
                    }
                }
                resolve(true);
            } catch (error) {
                console.log(error)
                return reject('error =>', error.message)
            }
        });


    });
}

async function cargarProveedoresAnexo(ruta) {
    return new Promise((resolve, reject) => {
        fs.readFile(ruta, 'utf8', async (err, data) => {
            if (err) throw err;
            xmlData = data;

            try {
                var jsonObj = parser.parse(xmlData, { parseTrueNumberOnly: true }, true);

                const ruc = jsonObj.iva.IdInformante.toString();
                const razonSocial = jsonObj.iva.razonSocial;
                const Anio = jsonObj.iva.Anio;
                const Mes = jsonObj.iva.Mes;

                if (jsonObj.iva.compras) {
                    for (var compras of Object.entries(jsonObj.iva.compras.detalleCompras)) {
                        var compra = compras[1];

                        var tipoProveedor = '0';
                        if (ruc.toString().length == 13) {
                            tipoProveedor = "01";
                        } else if (ruc.toString().length == 10) {
                            tipoProveedor = "02";
                        } else {
                            tipoProveedor = "03";
                        }

                        const existe = await ExisteProveedor(compra.idProv.toString(), ruc);
                        console.log(existe);
                        if (existe !== true) {
                            console.log('no existe');
                            const resp = await InsertarProveedor(compra.idProv.toString(), ruc);
                            // db.serialize(function () {
                            //     var stmt = db.prepare("INSERT INTO com_proveedores_ats (id_proveedor,tipo_proveedor,razonsocial_proveedor,actividad_proveedor,obligado_proveedor,nomcomercial_proveedor,informante) VALUES (?,?,?,?,?,?,?)");
                            //     stmt.run([compra.idProv.toString(), tipoProveedor, null, null, null, null, ruc]);
                            //     stmt.finalize();
                            // });
                        }

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
    for (const file of files) {
        const resp = await cargarProveedoresAnexo(file);
    }
    console.log('Archivos cargados con exito');
    // db.close();
}

const ExisteProveedor = (ruc, informante) => {
    return new Promise((resolve, reject) => {
        // console.log(ruc, informante);
        // var proveedor;
        // var tipoProveedor = '0';
        // if (ruc.toString().length == 13) {
        //     tipoProveedor = "01";
        // } else if (ruc.toString().length == 10) {
        //     tipoProveedor = "02";
        // } else {
        //     tipoProveedor = "03";
        // }

        db.get(`SELECT * FROM com_proveedores_ats where id_proveedor = '${ruc}' and informante = '${informante}'`, function (err, data) {
            if (data) {
                // proveedor existe, no se hace nada
                resolve(true);
            } else {
                resolve({ ruc, informante });
                // http.get(`http://181.199.71.180:8000/ruc/getRucApp.php?id=${ruc.toString()}`, (resp) => {
                //     var data;
                //     resp.on('data', (chunk) => {
                //         data = chunk;
                //     });
                //     resp.on('end', () => {
                //         proveedor = JSON.parse(data.toString().replace('undefined', ''));
                //         // console.log(proveedor);
                //         db.serialize(function () {
                //             var stmt = db.prepare("INSERT INTO com_proveedores_ats (id_proveedor,tipo_proveedor,razonsocial_proveedor,actividad_proveedor,obligado_proveedor,nomcomercial_proveedor,informante) VALUES (?,?,?,?,?,?,?)");
                //             stmt.run([ruc, tipoProveedor, proveedor.RAZON_SOCIAL, proveedor.ACTIVIDAD_ECONOMICA, proveedor.OBLIGADO, proveedor.NOMBRE_COMERCIAL, informante]);
                //             stmt.finalize();
                //         });
                //         resolve(true);
                //     });
                // }).on("error", (err) => {
                //     console.log("Error: " + err.message);
                //     reject(false);
                // });
            }
        });
    });
}

const InsertarProveedor = (ruc, informante, parteRelacionada) => {
    return new Promise((resolve, reject) => {

        var proveedor;
        var tipoProveedor = '0';
        if (ruc.toString().length == 13) {
            tipoProveedor = "01";
        } else if (ruc.toString().length == 10) {
            tipoProveedor = "02";
        } else {
            tipoProveedor = "03";
        }

        http.get(`http://186.4.146.196:8001/ruc/getRucApp.php?id=${ruc.toString()}`, (resp) => {
            var data;
            resp.on('data', (chunk) => {
                data = chunk;
            });
            resp.on('end', () => {
                proveedor = JSON.parse(data.toString().replace('undefined', ''));
                // console.log(proveedor);
                db.serialize(function () {
                    var stmt = db.prepare("INSERT INTO com_proveedores_ats (id_proveedor,tipo_proveedor,razonsocial_proveedor,actividad_proveedor,obligado_proveedor,nomcomercial_proveedor,informante) VALUES (?,?,?,?,?,?,?)");
                    stmt.run([ruc, tipoProveedor, proveedor.RAZON_SOCIAL, proveedor.ACTIVIDAD_ECONOMICA, proveedor.OBLIGADO, proveedor.NOMBRE_COMERCIAL, informante]);
                    stmt.finalize();
                });
                resolve(true);
            });
        }).on("error", (err) => {
            console.log("Error: " + err.message);
            reject(false);
        });
    });
}


IniciarCarga();