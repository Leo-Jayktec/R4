USE [cob_remesas]
GO

IF OBJECT_ID('dbo.atm_cons_result_p2p') IS NOT NULL
BEGIN
	DROP PROCEDURE dbo.atm_cons_result_p2p 

	IF OBJECT_ID('dbo.atm_cons_result_p2p') IS NOT NULL
		PRINT '<<< FAILED DROPPING PROCEDURE dbo.atm_cons_result_p2p  >>>'
	ELSE
		PRINT '<<< DROPPED PROCEDURE dbo.atm_cons_result_p2p  >>>'
END
go

CREATE PROCEDURE [dbo].[atm_cons_result_p2p] (
	@i_telefono_org   varchar(12) = null,
	@i_telefono_dest  varchar(12) = null,
	@i_referencia     varchar(12)= null,
	@i_fecha_tran     varchar(10) = null,
	@i_banco          varchar(10) = null,
	@o_return         VARCHAR(2) = '' OUT,
	@o_referencia     VARCHAR(12) = '' OUT
)
AS
BEGIN
	DECLARE @w_id			INT,
	        @w_result		VARCHAR(10),
			@w_referencia 	INT,
			@w_telefono_org   varchar(12),
			@w_telefono_dest  varchar(12),
			@w_banco          varchar(4)
			
	SET @o_referencia = @i_referencia
	SET @w_telefono_org = '58' + right(@i_telefono_org,10)
    SET @w_telefono_dest = '58' + right(@i_telefono_dest,10)
	SET @w_banco = '0' + RIGHT(@i_banco,3)
	
	SELECT @o_return = isnull(pm_codigo_respuesta,'12')
	FROM   cob_remesas..re_pago_movil
	WHERE  pm_telefono_o = @w_telefono_org
	AND    pm_telefono_d = @w_telefono_dest
	AND    right(pm_referencia,9) = right(@i_referencia,9)
	AND    pm_banco = @i_banco
	AND    convert(varchar(10),pm_fecha,101) = @i_fecha_tran

	IF ISNULL(@o_return,'') = ''
		SET @o_return = '12'
END

RETURN 0
GO
IF OBJECT_ID('dbo.atm_cons_result_p2p') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.atm_cons_result_p2p >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.atm_cons_result_p2p >>>'
go
