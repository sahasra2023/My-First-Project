
SELECT  CODE,to_char(to_date(CODE, 'yyyy-mm-dd'), 'DD-MON-YYYY') FROM AIRBNB_COMMON.XXABB_LOOKUP_TYPES WHERE TYPE = 'CURRENT_DS';
select *  from AIRBNB_COMMON.XXABB_LOOKUP_TYPES WHERE Type IN('BRAAVOS_ODI_MASTER','BRAAVOS_FILEGEN_MASTER');

select * from airbnb_common.xxabb_ds_process_flow  WHERE ds_date='05-may-2020' ;
select * from airbnb_common.xxabb_braavos_int_session_log WHERE 1=1  and ds='05-may-2020' aND int_sess_run_id=132736;


select * from airbnb_common.xxabb_ds_process_val_queries order by stage,sql_val_code;--where stage='SCH_FBDI';
select * from airbnb_common.xxabb_ds_process_val_threshold order by stage,int_code ;--where stage='SCH_FBDI';
select *   from airbnb_common.xxabb_ds_process_val_results where ds='05-MAY-2020';-- and stage ='SCH_FBDI';

SELECT *    from  AIRBNB_STAGING.XXABB_BRAAVOS_TRX_DATA_STG where ds='05-may-2020'; 
SELECT *    from  AIRBNB_STAGING.XXABB_BRAAVOS_XCH_RATES_STG where ds='05-may-2020' ;
SELECT *    from  AIRBNB_STAGING.XXABB_ENTITY_DERIVATION_STG where ds='05-may-2020' ;
SELECT *    from  AIRBNB_STAGING.XXABB_ECONOMIC_EVENT_DER_STG where ds='05-may-2020';
SELECT *    from  airbnb_error.xxabb_braavos_error_tbl where ds='05-may-2020' ;
SELECT *    from  airbnb_error.xxabb_braavos_error_summary where ds='05-may-2020' ;

select *   from  AIRBNB_PREPARE.XXABB_BRAAVOS_FBDI_HDR where ds='05-may-2020' ; 
SELECT *    from  AIRBNB_PREPARE.XXABB_BRAAVOS_FBDI_LINE where ds='05-may-2020';

SELECT *    from  AIRBNB_PREPARE.XXABB_BRAAVOS_FBDI_EXTRACTS where ds='05-may-2020' ;


SELECT AIRBNB_COMMON.XXABB_BRAAVOS_ENTITY_RULES_FUN('Homes','Create','14-04-20','US','US','US','','UrbanDoor') FROM DUAL;

SELECT * from AIRBNB_COMMON.XXABB_ENTITY_DER_LOOKUP 
WHERE '20-APR-2020'  between START_DATE and nvl(end_date, to_date('01-JAN-4179', 'DD-MON-YYYY')) and
entity_type is null or entity_type='UrbanDoor' ;

SELECT	'$' || LISTAGG( RULE_TYPE||'_'||ENTITY||'$'|| RULE_TYPE||'L_'||LEDGER  , '$') WITHIN GROUP (ORDER BY	RULE_TYPE||'_'||ENTITY||'$'|| RULE_TYPE||'L_'||LEDGER  ) ||'$' as Entity_Ledger 
FROM (
select  case when ENTITY =' or LEDGER=' then 'E' end status,ENTITY,LEDGER, decode(RULE_TYPE,'Guest','GOE','Host','HOE','PCOR','PCORE','TOT','TOTE', 'Employee','EOE') RULE_TYPE
, row_number() over(partition by RULE_TYPE order by GUEST_COR, HOST_COR,LISTING_COR,EMP_LOC) rn ,GUEST_COR, HOST_COR,LISTING_COR,EMP_LOC 
from AIRBNB_COMMON.XXABB_ENTITY_DER_LOOKUP 
WHERE '14-APR-2020'  between START_DATE and nvl(end_date, to_date('01-JAN-4179', 'DD-MON-YYYY')) 
and (INSTR(PRODUCT_TYPE ,'Homes')>0  OR INSTR(UPPER(PRODUCT_TYPE) , 'ALL')>0) 
and ( SOURCE = DECODE ( 'Homes' , 'HotelTonight', 'HotelTonight' , SOURCE) ) 
and (GUEST_COR is null    OR (( GUEST_COR_TYPE_OPR='IN'  AND instr(GUEST_COR,'US')>0) or ( GUEST_COR_TYPE_OPR='NOT IN' AND instr(GUEST_COR,'US')=0 )))
and (HOST_COR is null     OR (( HOST_COR_TYPE_OPR='IN'  AND instr(HOST_COR,'US')>0) or ( HOST_COR_TYPE_OPR='NOT IN' AND instr(HOST_COR,'US')=0 ))) 
and (LISTING_COR is null  OR (( LISTING_COR_TYPE_OPR='IN'  AND instr(LISTING_COR,'US')>0) or ( LISTING_COR_TYPE_OPR='NOT IN' AND instr(LISTING_COR,'US')=0 ))) 
and (EMP_LOC is null  OR (( EMP_LOC_TYPE_OPR='IN' 
AND instr('',EMP_LOC)>0) or ( EMP_LOC_TYPE_OPR='NOT IN' AND instr('',EMP_LOC)=0 ))) 
--and (entity_type is null or entity_type='UrbanDoor' )
and NVL(entity_type,'X') =  NVL ( 'UrbanDoor' , 'X' )
order by GUEST_COR, HOST_COR,LISTING_COR,EMP_LOC 
) where rn=1 ;