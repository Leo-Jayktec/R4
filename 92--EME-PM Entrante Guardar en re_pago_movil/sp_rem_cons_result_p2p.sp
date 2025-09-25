
USE cob_remesas
go
IF OBJECT_ID('dbo.sp_rem_cons_result_p2p') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.sp_rem_cons_result_p2p
    IF OBJECT_ID('dbo.sp_rem_cons_result_p2p') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.sp_rem_cons_result_p2p >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.sp_rem_cons_result_p2p >>>'
END
go
/********************************************************************************************/
/*				   BITACORA DE MODIFICACIONES												*/
/********************************************************************************************/
/*	FECHA: Septiembre/2025																	*/
/*	AUTOR: JAYKTEC                                  										*/
/*  RAZON: Consulta por parametro de la re_pago_movil        							 	*/
/********************************************************************************************/

create proc sp_rem_cons_result_p2p (
	@i_telefono_org		varchar(20)    	= null,
	@i_telefono_des		varchar(20)		= null,
	@i_referencia		varchar(20)	    = null,
	@i_banco		    char(4)		= null,
	@i_fecha_tran		datetime    = null,
	@o_resultado		varchar(2)		out,
	@o_referencia	varchar(200)		out
)
as
begin

declare @w_sp_name		varchar(32),
    @SQL			varchar(8000),
	@w_return		int,
    @w_telefono_org		varchar(20),
	@w_telefono_des		varchar(20),
	@w_referencia		varchar(20),
	@w_banco		    char(4),
	@w_fecha_tran		datetime
    
	SET @o_resultado = NULL
    SET @o_referencia = @i_referencia
	SET @w_telefono_org = '58' + right(@i_telefono_org,10)
    SET @w_telefono_des = '58' + right(@i_telefono_des,10)
	SET @w_banco =  RIGHT(@i_banco,3)
    
    SELECT TOP 1 @o_resultado = isnull(pm_codigo_respuesta,'12')
	FROM   cob_remesas..re_pago_movil
	WHERE  pm_telefono_o = @w_telefono_org
	AND    pm_telefono_d = @w_telefono_des
	AND pm_referencia LIKE '%' + right(@i_referencia,9)
	AND pm_banco LIKE '%' + @w_banco
    AND    pm_fecha = @i_fecha_tran
    AND    pm_reverso = 'N'
    ORDER BY pm_hora desc

    IF ISNULL(@o_resultado,'') = ''
		SET @o_resultado = '12'

end
go

IF OBJECT_ID('dbo.sp_rem_cons_result_p2p') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.sp_rem_cons_result_p2p >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.sp_rem_cons_result_p2p >>>'
go
