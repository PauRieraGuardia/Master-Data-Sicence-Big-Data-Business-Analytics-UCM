// CLASE II Práctica MONGODB (Agregación)

// 1. ETAPAS

db.inventoryE.find() // Base de datos que trabajamos en la otra clase

// operador $match

var query1 = {"item":"camiseta"} // Definición variable para hacer una query donde sea camiseta el item
var fase1 = {$match:query1} // Definición variable con una clave valor con operador $match y la query1, donde tiene que coincidir con la query para filtrar documentos 
var etapas = [fase1] // Crea una array que contiene las etapas de agregación que queremos aplicar
db.inventoryE.aggregate(etapas) //aplica etapas, que a su vez es fase1 a la base de datos inventoryE, se quedara con aquellas que sea item = camiseta (en este caso lo podriamos hacer con un find())
// La función aggregate() sirve para apicar querys de forma secuencial

// operador $project sirve para mostrar o ocultar campos

var query1 = {"item":"camiseta"} // Creamos variable para filtrar por item=camiseta
var fase1 = {$match:query1} // Creamos variable que filtra con el operador $match
var query2 = {"_id":false,"qty":false} // Creamos variable para determinar aquellos que no tengan id ni qty
var fase2 = {$project:query2} // Aplicamos operador project para ocultar o mostrar los datos
var etapas = [fase1,fase2] // Creamos una nueva variable para incluir en un array las distintas etapas
db.inventoryE.aggregate(etapas) // Aplicamos ambas querys con la función aggregate

// operador $addFields, se usa para agregar nuevos campos o modificar campos existentes

var query1 = {"item":"camiseta"} 
var fase1 = {$match:query1} 
var query2 = {"_id":false,"qty":false}  
var fase2 = {$addFields:query2} // aplicamos operador $addFields para crear o modificar campos, podemos usar el $set para realizar exactamente lo mismo
var etapas = [fase1,fase2] 
db.inventoryE.aggregate(etapas)
// !! La diferencia entre $set y $proyect es que $set modifica los campos y el $project simplemente muestra

// operador $group, para agrupar

var query1 = {"_id":"$item","numero":{$sum:1 }} //Creamos una variable con una clave valor id:item (de ahí el "nuevo operador" $item) y creamos una nueva clave valor llamada numero aplicando el operador $sum
var fase1 = {$group:query1} // Aplicamos operador $group para agrupar en función de la query1
var etapas = [fase1] //creamos array para aplicarlo a la función aggregate
db.inventoryE.aggregate(etapas)

// operador $sort, para ordenar

var query1 = {"_id":"$item","numero":{$sum:1 }} 
var fase1 = {$group:query1}
var query2 = {"numero":-1,"_id": 1} // Creamos variable para ordenar de modo descendente (ascendente sería 1), en número y ascendente en letras en _id
var fase2 = {$sort:query2}
var etapas = [fase1,fase2] 
db.inventoryE.aggregate(etapas)

// operador $limit, para limitar 

var query1 = {"_id":"$item","numero":{$sum:1 }} 
var fase1 = {$group:query1}
var query2 = {"numero":-1,"_id": 1}
var fase2 = {$sort:query2}
var fase3 = {$limit:1} // Límita aquel que tiene más productos
var etapas = [fase1,fase2, fase3] 
db.inventoryE.aggregate(etapas)

// operador $unwind, desagrupa datos agrupados

db.inventoryAE.find() 

var fase1 = {$unwind:"$size"} //creamos variable para desagrupar los datos en size
var etapas = [fase1] // creamos array etapas para aplicar los filtros
db.inventoryAE.aggregate(etapas)

// operador $out, se usa para guardar los resultados de la agregación en una colección

var fase1 = {$unwind:"$size"}
var query2 = {"_id":0} // Debemos usar esta query para que no muestre el ID, y por lo tanto generará un nuevo ID para cada uno
var fase2 = {$project:query2} 
var fase3 = {$out:"borrar"} // Creamos una variable para crear una nueva colección llamada borrar, se guardará en la base de datos 
var etapas = [fase1,fase2,fase3]
db.inventoryAE.aggregate(etapas)

// EJERCICIO 1:


// Filtrar por camiseta

var query1 = {"item":"camiseta"}
var fase1 = {$match:query1}
db.inventoryE.aggregate(fase1)

// Añadir nuevo campo importe total (qty*price)

var subquery1 = {$multiply: ["$qty":"$price"}}
var query = {"importe total" : subquery1}
var fase1 = {$set: query}
var etapas = [fase1]
db.inventoryE.aggregate(etapas)
 
// contar cuantas camisetas hay

var query1 = {"item":"camiseta"}
var fase1 = {$match:query1}
var query2 = {"_id":"$item","numero":{$sum:1}}
var fase2 = {$group:query2}
var etapas = [fase1,fase2]
db.inventoryE.aggregate(etapas)

// La suma de sus importes totales
var subquery0 = {$multiply: ["$qty":"$price"}}
var query0 = {"importe total" : subquery0}
var fase0 = {$set: query0}
var query1 = {"item":"camiseta"}
var fase1 = {$match:query1}
var query2 = {"_id":"$item","numero":{$sum:1},"totales":{$sum:"$importe total"}}
var fase2 = {$group:query2}
var etapas = [fase0,fase1,fase2]
db.inventoryE.aggregate(etapas)

//El mínimo de price

var subquery0 = {$multiply: ["$qty":"$price"}}
var query0 = {"importe total" : subquery0}
var fase0 = {$set: query0}
var query1 = {"item":"camiseta"}
var fase1 = {$match:query1}
var query2 = {"_id":"$item","numero":{$sum:1},"minimo":{$min:"$price"}}
var fase2 = {$group:query2}
var etapas = [fase0,fase1,fase2]
db.inventoryE.aggregate(etapas)

// El máximo de price

var subquery0 = {$multiply: ["$qty":"$price"}}
var query0 = {"importe total" : subquery0}
var fase0 = {$set: query0}
var query1 = {"item":"camiseta"}
var fase1 = {$match:query1}
var query2 = {"_id":"$item","numero":{$sum:1},"maximo":{$max:"$price"}}
var fase2 = {$group:query2}
var etapas = [fase0,fase1,fase2]
db.inventoryE.aggregate(etapas)

// Agrupar por item y añadir un array de documentos que tengan cada qty y price asociado

var query1 = {"_id":"$item", cantidades : {$push:{"qty":"$qty","price":"$price"}}} // La función push agrega elementos a un array
var fase1 = {$group:query1}
var etapas =  [fase1]
db.inventoryE.aggregate(etapas)

// Agrupar por item y tag (transformando a mayúsculas) y contar documentos, ordenar de forma descendente por el conteo

var query1 = {"_id":{"item":{$toUpper:"$item"},"tag":{$toUpper:"$tag"}},total:{$sum:1}} // el operador $toUpper pone en mayusculas los elementos
var fase1 = {$group:query1}
var query2 = {"tot":-1}
var fase2 = {$sort:query2}
var etapas = [fase1,fase2]
db.inventoryE.aggregate(etapas)



