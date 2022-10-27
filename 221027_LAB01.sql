drop database if exists ETL_Lab01;
create database ETL_Lab01;

use ETL_Lab01;

create view Ps_Total  as 
select * from ps_20200413 union 
select * from ps_20200419 union
select * from ps_20200426 union
select * from ps_20200503 union
select * from ps_20200518 ;



select avg(precio), Id_Sucursal
from Ps_Total
group by Id_Sucursal
having Id_Sucursal = 09010688;

select avg(precio), Id_Sucursal
from ps_20200518 
group by Id_Sucursal
having Id_Sucursal = 09010688;