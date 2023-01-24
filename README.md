# test-td-dataeng

Assignment for TD

Qusetion 1 - Data Pipeline Design

![TD-Q1](https://user-images.githubusercontent.com/38657478/214230538-7ce8d647-d2bf-41eb-ae52-20b8511f2f2d.jpg)


Question 2 - Text Sanitizer


Question 3 - SQL : BIgQuery SQL syntax

```
with sale_transaction as (
  select 1 as transaction_id , 1 as product_id , 5 as quantity union all
  select 1, 2, 7 union all
  select 2, 3, 1 union all
  select 3, 7, 3 union all
  select 4, 4, 1 union all
  select 4, 5, 1 union all
  select 5, 6, 3 
)
, product as (
  select 1 as product_id, "aa" as product_name , 10 as retial_price , 1 as product_class_id union all
  select 2 , "bb" , 20,1 union all
  select 3 , "cc" , 30,2 union all
  select 4 , "dd" , 40,3 union all
  select 5 , "ee" , 50,3 union all
  select 6 , "ff" , 45,2 union all
  select 7 , "gg" , 35,3

)
, product_class as (
  select 1 as product_class_id , "Class A" as product_class_name union all
  select 2 , "Class B" union all
  select 3 , "Class C"
)
, cte_sum_product as (
select 
  pc.product_class_name
  , p.product_name
  , sum(st.quantity*p.retial_price) as sales_value
  , dense_rank() over(partition by product_class_name order by sum(st.quantity*p.retial_price) desc ) as rank
  from sale_transaction as st
  left join product p on st.product_id = p.product_id
  left join product_class pc on p.product_class_id = pc.product_class_id
  group by pc.product_class_name, p.product_name
)
select product_class_name,rank , product_name,sales_value from cte_sum_product
where cte_sum_product.rank <=2
order by product_class_name,sales_value desc

```
