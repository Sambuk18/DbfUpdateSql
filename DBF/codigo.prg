* Abrimos las tablas y creamos los índices
USE recibos EXCLUSIVE
INDEX ON STR(pronro, 4) + STR(numero, 8) TAG IndiceR
USE con_gast EXCLUSIVE
INDEX ON STR(proint, 4) + STR(gas_nrecib, 8) TAG IndiceC

* Aseguramos que ambas tablas estén abiertas
USE recibos IN 1
USE con_gast IN 2

* Activamos el índice en ambas tablas
SELECT 1
SET ORDER TO TAG IndiceR
SELECT 2
SET ORDER TO TAG IndiceC

* Seleccionamos la tabla recibos
SELECT 1

* Recorremos todos los registros de recibos
SCAN
    * Generamos la clave compuesta
    lcClave = STR(pronro, 4) + STR(numero, 8)

    * Guardamos el valor de num_cli
    lnNumCli = num_cli

    * Cambiamos a con_gast
    SELECT 2

    * Buscamos el registro correspondiente en con_gast
    SEEK lcClave
    IF FOUND()
        * Si se encuentra, actualizamos el campo client con el valor de num_cli
        REPLACE client WITH lnNumCli
    ENDIF

    * Volvemos a la tabla recibos
    SELECT 1
ENDSCAN

* Cerramos las tablas
USE IN 1
USE IN 2

* Mensaje de confirmación
@ 10,10 SAY "Proceso completado correctamente"
RETURN
