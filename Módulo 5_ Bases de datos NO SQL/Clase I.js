// CLASE I Práctica Mongo DB 

show dbs // Este codigo nos muestra las bases de datos exitentes

db.getName() // Nos da el nombre de la pesaña de base de datos que estamos utilizando, en nuestro caso clases

use clases // Usamos la base de datos test, se creara la base de datos pork no existia

show collections // Muestra las colecciones dentro de la base de datos, en nuestro caso las que hay dentro de clases

// 1. BORRADOR INICIAL, para eliminar todo y empezar de nuevo

db.inventory.drop()
db.inventoryA.drop() 
db.inventoryAE.drop()
db.inventoryE.drop()

// 2. INSERTS, insertar valores al inventario

var nuevo_item = {"item":"camiseta","qty":100,"price":19.5,"tag":"algodon","size_h":28,"size_w":35,"size_uom":"cm"} // Definimos una variable con una serie de clave : valor llanada nuevo_item

db.inventory.insertOne(nuevo_item) // insteramos en el inventario de nuestra base de datos actual (clases) la nueva variable definida.

db.inventory.find() //Nos devuelve todo lo que encontramos en el inventario en este caso nos devolvera, además, el _id que se ha generado al insertar la nueva variable al inventory

var array_items = [
{ "item": "camiseta", "qty": 100, "price": 19.5, "tag": "algodon", "size_h": 28, "size_w": 35, "size_uom": "cm" } ,

{ "item": "camiseta", "qty": 100, "price": 19.5, "tag": "algodon", "size_h": 11.02, "size_w": 13.78, "size_uom": "pu" } ,

{ "item": "medias", "qty": 10, "price": 1.5, "tag": "licra", "size_h": 2, "size_w": 3, "size_uom": "cm" } ,

{ "item": "corbata", "qty": 6, "price": 5.5, "tag": "algodon", "size_h": 5, "size_w": 4, "size_uom": "cm" } ,

{ "item": "camiseta", "qty": 78, "price": 10.5, "tag": "sintetico", "size_h": 4, "size_w": 1, "size_uom": "cm" }
    
] // Definimos un array con multiples variables en clave:valor para añadir a nuestro inventario en la base de datos clase

db.inventory.insertMany(array_items) // Insertamos el array al inventario de la base de datos clases
db.inventory.find() // Observamos todas las variables creadas hasta ahora

// 3. FIND, misma funcionalidad que where en SQL

var where = {"item":"camiseta"} // definimos una variable "item":"camiseta"
db.inventory.find(where) // Utilizamos la función find() con la variable where, anteriormente creada, para que nos encuentra aquellas variables dentro de nuestro inventario que coincidan con la variable query

// IMP!! Las variables solo se definen una vez las corremos, por lo tanto, debemos seleccionar ambas líneas de código

var where = {"item":"camiseta","qty":78}
db.inventory.find(where)


// 4. PROYECCIONES, misma función que select en SQL

var where = {"item":"camiseta","qty":78} // definimos variable wher0e, para filtrar como anteriormente
var select = {"qty":1} // Definimos variable select, que sera aquella variable que queremos que nos muestre, es lo mismo 1 que true.
db.inventory.find(where,select) // Buscamos dentro de inventory en la base de datos que estemos usando ambas variables, donde deifinimos la variable where y la variable select

var query = {}
var select = {"_id":false,"qty":true}
db.inventory.find(query,select) // Este código nos muestra todos los qty, sin mostrar el id que por defecto se muestra, de todas las variables de nuestro inventario

var query = {}
var select = {"_id":false,"qty":0}
db.inventory.find(query,select) // En este nos oculta las seleccionadas y nos muestra el resto.

// 5. UPDATE, para modificar los datos del inventario

var where = {} // Este es el where donde seleccionaremos todas las variables 

var operacion = {$set : {"campo":"borrar"}} // creamos una variable llamada operation con $set para actualizar los datos, con una nueva clave llamada "campo" con valor "borrar"
// El set crea o remplaza la variable

db.inventory.updateMany(where,operacion) //Actualizamos con las variables definidas.

db.inventory.find() // Para ver si realmente se ha definido la nueva clave:valor 


// Remplazamos en donde se cumplan las siguiente caracteristicas en la llave "campo" el valor borrar" por "hola"
var where = {
	"item" : "camiseta",
	"qty" : 100,
	"price" : 19.5,
	"tag" : "algodon",
	"size_h" : 28,
	"size_w" : 35,
	"size_uom" : "cm"}
var operacion = {$set : {"campo":"hola"}}
db.inventory.updateMany(where,operacion) 
db.inventory.find() 

// 6. DELETE, para borrar distintas claves:valor

var nueva_variable = {"para":"borrar"} // Creamos una nueva variable para despúes eliminarla
db.inventory.insertOne(nueva_variable) // Insertamos esa variable a nuestro inventario
db.inventory.find() 

db.inventory.deleteMany(nueva_variable)

db.inventory.find()

// 7. FILTROS AVANZADOS

// 7.1 Selectores, para seleccionar claves mayores, menores o iguales

var query = {"qty":{$gt:78}} // Creamos variable para definir aquellos que sear mayores que 78 el qty

db.inventory.find(query)

var query = {"qty":{$lt:78}} // Creamos variable para definir aquellos que sean menores que 78 el qty

db.inventory.find(query)

var query = {"qty":{$ne:10}} // Creamos variable para definir aquellos que sean distintos que 10 el qty

db.inventory.find(query)

// 7.2 Lógicos 

var query1 = {"qty":{$ne:10}} // definimos variable que nos determina aquellos que sean distintos a 10 en qty
var query2 = {"qty":{$lt:70}} // definimos variabe que nos defina aquellos que sean menores que 70 en qty
var logic = {$and: [query1,query2]]} // Definimos variable llamada logic donde determinamos como clave $and y como valor [query1,query2], para decir que se deben cumplir las dos condiciones
db.inventory.find(logic) // Aplicamos find()

// IMP! Hay que tener en cuenta siempre el codigo tipo clave:valor, como si fuera un diccionario en python. Tener en cuenta que siempre los operadores estan en la parte de la clave

var query1 = {"qty":{$eq:100}} // definimos variable que nos determina aquellos que sean igaules a 100 en qty
var query2 = {"qty":{$lt:50}} // definimos variabe que nos defina aquellos que sean menores que 50 en qty
var logic = {$or: [query1,query2]]} // Definimos variable llamada logic donde determinamos como clave $or y como valor [query1,query2], para decir que se deben cumplir una de las dos condiciones
db.inventory.find(logic)


var query1 = {"qty":{$eq:100}} // definimos variable que nos determina aquellos que sean igaules a 100 en qty
var query2 = {"qty":{$lt:50}} // definimos variabe que nos defina aquellos que sean menores que 50 en qty
var logic = {$nor: [query1,query2]]} // Definimos variable llamada logic donde determinamos como clave $nor (not or) y como valor [query1,query2], para descartar aquellos que cumpla una de las dos caracteristicas (filtro opuesto al anterior)
db.inventory.find(logic)

// Si hacemos un union del or y del nor nos dan todos los valores, pork son complementarios

// 8. ELEMENTOS

var query = {"qty":{$exists:true}} //definimos una nueva variable donde nos determina si existe la variable qty mediante la clave valor {$exists:true}
db.inventory.find(query) // Aplicamos la variable query

var query = {"noexiste":{$exists:false}} // Creamos variable para encontrar aquellas variables que no existen definiendola con la clave noexiste
var operation = {$set : {"noexiste":null}} // Creamos una variable que cree una variable que tenga clave noexiste con valor null
db.inventory.updateMany(query,operation) // Aplicamos las dos variables la inventario, aquellos que no tengan la clave noexiste, crea la clave noexiste con valor null

db.inventory.find()

var query = {"qty":{$type:"double"}} // Definimos variable que nos muestra aquellos que sean tipo decimales en qty
db.inventory.find(query)

var query = {"qty":{$type:"int"}} // Definimos variable que nos muestra aquellos que sean tipo integers en qty
db.inventory.find(query)

// 9. DOCUMENTO EMBEBIDO, con subclaves

var array_items= [

{ "item": "camiseta", "qty": 100, "price": 19.5, "tag": "algodon", "size": { "h": 28, "w": 35, "uom": "cm" } } ,

{ "item": "camiseta", "qty": 100, "price": 19.5, "tag": "algodon", "size": { "h": 11.02, "w": 13.78, "uom": "pu" } },

{ "item": "medias", "qty": 10, "price": 1.5, "tag": "licra", "size": { "h": 2, "w": 3, "uom": "cm" } },

{ "item": "corbata", "qty": 6, "price": 5.5, "tag": "algodon", "size": { "h": 5, "w": 4, "uom": "cm" } },

{ "item": "camiseta", "qty": 78, "price": 10.5, "tag": "sintetico", "size": { "h": 4, "w": 1, "uom": "cm" } }

] // Creamos un nuevo array con distints valores y distintas clave:valor, vemos en size como aplicamos como valor un nuevo json con distintas clave valor

db.inventoryE.insertMany(array_items) // Creamos un nuevo inventario dentro de nuestra base de datos clase llamado inventoryE

db.inventoryE.find()

// 10. CONSULTA SOBRE SUBCLAVE

var query = {"size.h":11.02} // Definimos variable como size.h donde el valor sea 11.02, es decir la sub clave h de la clave size
db.inventoryE.find(query)

// 11. SOBRE VARIAS SUBCLAVES

var query = {"size.h":{$lt:15},"size.uom":"cm"}

db.inventoryE.find(query)

// 12. ARRAY 

var array_items= [

{ "item": "camiseta", "qty": 100, "price": 19.5, "tag": ["algodon","sintetico"], "size": { "h": 28, "w": 35, "uom": "cm" } } ,

{ "item": "camiseta", "qty": 100, "price": 19.5, "tag": ["algodon","sintetico"], "size": { "h": 11.02, "w": 13.78, "uom": "pu" } },

{ "item": "medias", "qty": 10, "price": 1.5, "tag": ["licra"], "size": { "h": 2, "w": 3, "uom": "cm" } },

{ "item": "corbata", "qty": 6, "price": 5.5, "tag": ["algodon","seda"], "size": { "h": 5, "w": 4, "uom": "cm" } },

{ "item": "camiseta", "qty": 78, "price": 10.5, "tag": ["sintetico","seda"], "size": { "h": 4, "w": 1, "uom": "cm" } }

] // Creamos nuevas variables con arrrays dentro, como en tag. Eso implica que tiene dos valores en una clave 

db.inventoryA.insertMany( array_items ) // Creamos un nuevo inventario y añadimos la variable array_items
db.inventoryA.find()

// find() para elementos de array en el orden, número de elementos y valor exacto para encontrar aquellos valores

var query = {tag : ["algodon","seda"]} // Creamos variable con clave tag y valores de la variable con el array que queremos encontrar
db.inventoryA.find(query)

// find() elementos del array pero sin orden con el operador $all

var query = {tag : {$all: ["algodon","seda"]}} // Creamos una nueva clave aplicando el operador $all con los valores del array
db.inventoryA.find(query)

// find() elementos del array que satisface al menos un valor o condición de todas ellas

var query = { tag: { $in: ["seda","licra"] } } // definimos clave valor con los valores y usando la como clave el operador $in
db.inventoryA.find( query )

// Elementos de array que satisface todas las condiciones

var query={ tag:{ $nin: ["seda","licra"] } } // definimos clave valor con los valores y usando la como clave el operador $nin
db.inventoryA.find( query )

// Un Elemento del array con posición 

var query = {"tag.0":"algodon"}
db.inventoryA.find(query)

// Consulta sobre el número de elementos del array

var query = {"tag":{$size:1}}
db.inventoryA.find(query)

// 13. ARRAY SUBDOCUMENTOS

var array_items= [

{ "item": "camiseta", "qty": 100, "price": 19.5, "tag": ["algodon","sintetico"], "size": [{ "h": 28, "w": 35, "uom": "cm" }, { "h": 11.02, "w": 13.78, "uom": "pu" }] } ,

{ "item": "medias", "qty": 10, "price": 1.5, "tag": ["licra"], "size": [ { "h": 2, "w": 3, "uom": "cm" } ] },

{ "item": "corbata", "qty": 6, "price": 5.5, "tag": ["algodon","seda"], "size": [ { "h": 5, "w": 4, "uom": "cm" }  ] },

{ "item": "camiseta", "qty": 78, "price": 10.5, "tag": ["sintetico","seda"], "size": [ { "h": 4, "w": 1, "uom": "cm" } ]}

]
db.inventoryAE.insertMany( array_items )  // Creamos un nuevo inventario con valores en array en tag y en size array y json
db.inventoryAE.find()

// Consulta sobre todos los documentos de un array, donde al menos un documento debe ser exactamente igual, valores, orden y número de campos/claves

var query={size: { "h": 28, "w": 35, "uom": "cm" } }
db.inventoryAE.find( query )

var query={size: { "h": 28, "w": 35 } }
db.inventoryAE.find( query )

var query={size: { "w": 35, "uom": "cm", "h": 28 } }
db.inventoryAE.find( query )

// Consulta sobre todos los documentos de un array, donde al menos un documento debe tener el campo/clave a filtarr pero no es necesario especificar el orden del mismo

var query = {"size.w":35}
db.inventoryAE.find(query)

// Consulta sobre un documento del arrayt (posición específica), donde debe tener el campo/clave a filtrar sin especificar el orden

var query = ["size.0.w":35]
db.inventoryAE.find(query)

// Consulta sobre todos los documentos de un array, donde al menos un documento satisface todas las condiciones

var query={ size: { $elemMatch: { "w": 35, "uom": "cm" } } }
db.inventoryAE.find( query )

// Consulta sobre todos los documentos de un array, donde cada condición se debe satisfacer aunque no sean sobre el mismo documento dentro del array

var query={ "size.h": { $gt: 13,  $lte: 30} }
db.inventoryAE.find( query )

// La próxima query no devuelve nada, no existe dentro del array size documentos que tengan el campo h para algún caso menor igual que 10 y mayor o igual que 30(el ejemplo de antes tiene 11.07 y 28) 

var query={ "size.h": { $gt: 30,  $lte: 10} }
db.inventoryAE.find( query )

// Consulta sobre todos los documentos de un array, donde al menos un documento satisface todas las condiciones

// No devuelve nada

var query={ "size":  { $elemMatch: { "h": { $gt: 12,  $lte: 27 } } } } // Este codigo se comporta como un between
db.inventoryAE.find( query )

// Update
// $addToSet

var query= { }
var operacion= { $addToSet: { "tag": "borrar" } }
db.inventoryA.updateMany(query,operacion)
db.inventoryA.find()

// $pop

var query= { }
var operacion= { $pop: { "tag": 1 } }
db.inventoryA.updateMany(query,operacion)
db.inventoryA.find()

// $push

var query= { }
var operacion= { $push: { "tag": "borrar" } }
db.inventoryA.updateMany(query,operacion)
db.inventoryA.find()

// $pull

var query= { }
var operacion= { $pull: { "tag": "borrar" } }
db.inventoryA.updateMany(query,operacion)
db.inventoryA.find()

// $each

var query= { }
var operacion= { $push: { "tag": { $each: ["borrar","borrar"] } } }
db.inventoryA.updateMany(query,operacion)
db.inventoryA.find()

var query= { }
var operacion= { $pull: { "tag": "borrar" } }
db.inventoryA.updateMany(query,operacion)
db.inventoryA.find()

// sort

var opcion= { "qty" : -1 }
db.inventory.find().sort(opcion)

// limit

var opcion= { "qty" : -1 }
db.inventory.find().sort(opcion).limit(1)

// count

db.inventory.find().count()

// toArray

db.inventory.find().toArray()



