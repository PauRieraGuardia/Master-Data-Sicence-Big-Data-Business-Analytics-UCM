// PACK EJERCICIO 1

db.arts.drop()

// Insertar varios documentos

var array_items = [{ "title" : "The Pillars of Society", "artist" : "Grosz", "year" : 1926, "price" : NumberDecimal("199.99") }, { "title" : "Melancholy III", "artist" : "Munch", "year" : 1902, "price" : NumberDecimal("280.00") }, { "title" : "Dancer", "artist" : "Miro", "year" : 1925, "price" : NumberDecimal("76.04") }, { "title" : "The Great Wave off Kanagawa", "artist" : "Hokusai", "price" : NumberDecimal("167.30") }, { "title" : "The Persistence of Memory", "artist" : "Dali", "year" : 1931, "price" : NumberDecimal("483.00") }, { "title" : "Composition VII", "artist" : "Kandinsky", "year" : 1913, "price" : NumberDecimal("385.00") }, { "title" : "The Scream", "artist" : "Munch", "year" : 1893 }, { "title" : "Blue Flower", "artist" : "O'Keefe", "year" : 1918, "price" : NumberDecimal("118.42") } ]

db.arts.insertMany(array_items) // Introducimos el array creado y creamos una nueva colección

db.arts.find() // En la siguiente colección encontramos el id, el título, el artísta, el año y el precio del cuadro

//1. Buscar Cuadros cuyo año esté por encima de 1920

var query = {year:{$gt:1920}}
db.arts.find(query)

//2. Buscar Cuadros cuyo año esté por encima de 1920 Y su precio sea mayor a 200

var query = {year:{$gt:1920},price:{$gt:200}}
db.arts.find(query)

//3. Buscar Cuadros cuyo año esté por encima de 1920 O su precio sea mayor a 200

var query1 = {year:{$gt:1920}}
var query2 = {price:{$gt:200}}
var logic = {$or:[query1,query2]}
db.arts.find(logic)

//4. Descartar cuadros que sean menores de 1925 y sean mayores de 1950 

var query1 = {year:{$lt:1925}}
var query2 = {year:{$gt:1950}}
var logic = {$nor:[query1,query2]}
db.arts.find(logic)

//5. Buscar Cuadros que NO ESTÉN DATADOS (no tengan el campo year)

var query = {year:{$exists:false}}
db.arts.find(query)

//6. Añadir campo year igual a NULL en campos que no estén datados

var query = {year:{$exists:false}}
var operation = {$set:{year:null}}
db.arts.updateMany(query,operation)

//7. Ordenar por año de forma descendente

var query = {year:-1}
db.arts.find().sort(query)

//8. Limitar la salida anterior a las 3 primeras filas

var query = {year:-1}
db.arts.find().sort(query).limit(3)

//9. Mostrar las filas de la cuarta a la sexta

var query = {year:-1}
db.arts.find().sort(query).skip(3).limit(3)

