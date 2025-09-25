USE cob_atm
go
IF OBJECT_ID('dbo.atm_val_p2p_ws') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.atm_val_p2p_ws
    IF OBJECT_ID('dbo.atm_val_p2p_ws') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.atm_val_p2p_ws >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.atm_val_p2p_ws >>>'
END
go
SET ANSI_NULLS ON
go
SET QUOTED_IDENTIFIER ON
go

/************************************************************/
/*  Archivo:            atm_val_p2p_ws.sp                   */
/*  Stored procedure:   atm_val_p2p_ws                      */
/*  Producto:           ATM                                 */
/************************************************************/
/*                          PROPOSITO                       */
/* Validar las transacciones de pago movil entrante         */
/************************************************************/
/*                        MODIFICACIONES                    */
/*  FECHA              AUTOR              RAZON             */
/*  05/09/2024         Jaykyec        EMISION INICIAL       */
/*  06/11/2024         Jayktec  C2P,Anulacion,Reverso       */
/*  03/02/2025         Jayktec  Extraer manejo de errores   */ 
/************************************************************/

CREATE PROCEDURE [dbo].[atm_val_p2p_ws] (
     @i_srv_org			VARCHAR(30) = ''
	,@i_corr			CHAR(1)     = 'N'					--N=No, S=Si-Reverso
	,@i_telefono_org	VARCHAR(12) = NULL
	,@i_cedula_orig		VARCHAR(20) = NULL	
	,@i_banco_org		VARCHAR(4) = NULL
	,@i_telefono_dest	VARCHAR(12) = NULL
	,@i_cedula_dest		VARCHAR(20) = NULL
	,@i_op_code			VARCHAR(8) = NULL
	,@i_bit37		    VARCHAR(12) = NULL				    --Referencia del Emisor (bit37)
	,@i_trace			VARCHAR(64) = NULL
	,@i_monto			MONEY = NULL
	,@i_otp             VARCHAR(8) = NULL
	,@o_retorno			VARCHAR(3) = NULL OUTPUT
	,@o_pl_error		INT = NULL OUTPUT
	,@o_mensaje			VARCHAR(200) = NULL OUTPUT
	,@o_cta_dest		VARCHAR(24) = NULL OUTPUT
)
AS
BEGIN
	DECLARE  @o_sld_girar     MONEY
			,@w_ssn_host      INT
			,@w_num_error     INT
			,@w_ssn_branch    INT
			,@w_referemcia    INT
			,@w_sec_reverso   INT
			,@w_oficina_org   SMALLINT
			,@w_prod_habilitado_AH CHAR(1)
			,@w_prod_habilitado_CC CHAR(1)
			,@w_banco_pago    INT
			,@w_banco         CHAR(3)
			,@w_srv_local     VARCHAR(30)
			,@w_bit37    	  INT				
			,@w_op_code 	  VARCHAR(8)
			,@w_corr 		  CHAR(1)
			,@w_tipo_pago 	  VARCHAR(3)
			,@w_return 		  INT
			,@w_monto		  MONEY
			,@w_banco_char    CHAR(4)
			,@w_banco_r4      CHAR(4)
			,@w_ente_orig     INT
			,@w_monto_min	  MONEY
			,@w_monto_max	  MONEY
			,@w_fecha_proc    DATETIME
			,@w_ente_dest     INT
			,@w_ente_mis_dest INT
			,@w_ente_adm	  INT
			,@w_nom_cliente_benef	VARCHAR(64)
		    ,@w_producto_dest		SMALLINT
		    ,@w_cta_banco_dest		VARCHAR(20)
			,@w_canal				SMALLINT
			,@w_tarjeta         varchar(16) 
			,@w_tipo_cupo       char(1)     
			,@w_cupo            tinyint     
			,@w_comision        money       
			,@w_afecta_cupo     char(1)     
			,@w_saldo_cupo_new  money       
			,@w_fecha_cupo_new  datetime    
			,@w_num_trans_new   int    
			,@w_socio           char(1)
			,@w_retorno			char(2)
			,@w_habilitado      int
			,@w_ing_tran        char(1)
			,@w_ofi_cta_ori     INT
			,@w_result          CHAR(2)
			,@w_telefono_org	VARCHAR(12)
			,@w_telefono_dest	VARCHAR(12)
			,@w_cedula_dest     VARCHAR(20)
			,@w_validar_banco	TINYINT
			,@w_param_mto_min   VARCHAR(10)
			,@w_param_mto_max   VARCHAR(10)
			,@w_signo           CHAR(1)		
			,@w_telefono_soc	VARCHAR(12)
			,@w_tipo_cliente    CHAR(1)
			,@w_manejo_cola     CHAR(1)
			,@w_autorizado		CHAR(1)
			,@w_tipo            VARCHAR(12)
			,@w_fecha_log		DATETIME

	BEGIN TRY		
		IF @i_corr = 'S'
			SET @w_corr = 'R'
		ELSE
			SET @w_corr = @i_corr

		SET @w_tipo = 'RECIBIDA'
		SET @w_signo = 'C'

		IF @i_op_code = '560009' 
		BEGIN
			SET @w_tipo = 'ENVIADA'		
			SET @w_signo = 'D'	
		END

		--Inicializar variables
		SET @w_ssn_branch = 0
		SET @w_ing_tran = 'N'
		SET @w_socio = 'N'
		SET @w_ente_mis_dest = 0
		SET @w_bit37 = convert(INT, right(@i_bit37, 9))
		SET @i_op_code = left(@i_op_code, 6)
		SET @w_op_code = @i_op_code
		SET @w_monto = convert(money, @i_monto)
		SET @w_canal = 4
		SET @w_telefono_org = '0' + right(@i_telefono_org,10)
		SET @w_telefono_dest = '0' + right(@i_telefono_dest,10)
		SET @w_telefono_soc = @i_telefono_dest
		SET @w_manejo_cola = 'N'	

		--Inicializar variables de Salida
		SET @o_retorno = NULL
		SET @o_pl_error = NULL
		SET @o_mensaje = NULL
		SET @o_cta_dest = NULL	
		
		--Oficina asociada a la RED
		SELECT @w_oficina_org = ro_oficina
		FROM   cob_atm..tm_redes_oficinas
		WHERE  ro_red = @i_srv_org

		-- Obtiene el nombre del servidor local
		SELECT @w_srv_local = pa_char
		FROM   cobis..cl_parametro
		WHERE  pa_producto = 'BV'
		AND    pa_nemonico = 'BVSRV'	

		--Codigo identificacion R4
		SELECT @w_banco_r4 = pa_char
		FROM   cobis..cl_parametro
		WHERE  pa_producto = 'BV'
		AND    pa_nemonico = 'IDMIBA'

		--Fecha de Proceso
		SELECT @w_fecha_proc = fp_fecha
		FROM   cobis..ba_fecha_proceso	

		SET @w_tipo_pago = CASE 
							WHEN @i_op_code = '560000'
								THEN 'P2P'
							WHEN @i_op_code = '560009'
								THEN 'C2P'
							WHEN @i_op_code = '560050'
								THEN 'P2C'
							WHEN @w_op_code = '200030'    --JT-30102024 Anulacion
								THEN 'C2P'							
							END

		--Consultar si el destino es cliente del banco con la identificacion envida
		SELECT @w_cedula_dest = en_ced_ruc
		FROM   cob_bvirtual..bv_ente
		WHERE  en_ced_ruc = @i_cedula_dest

		--Si no existe se eliminan los posibles 0 a la izquierda
		IF @w_cedula_dest IS NULL
			select @i_cedula_dest = convert(VARCHAR, convert(NUMERIC, cob_atm.dbo.numero(@i_cedula_dest)))

		--Ingreso de informacion en el Log  
		INSERT INTO tm_p2p_log (
			pl_corr    ,pl_op_code ,pl_banco ,pl_telefono_o ,pl_telefono_d
			,pl_cedula ,pl_trace   ,pl_monto ,pl_tipo_pago  ,pl_referencia ,pl_procesar)
		VALUES (
			@w_corr         ,@i_op_code  ,@i_banco_org ,@i_telefono_org ,@i_telefono_dest
			,@i_cedula_dest ,@i_trace    ,@w_monto     ,@w_tipo_pago    ,@i_bit37			,NULL) 

		IF @@ERROR > 0
		BEGIN
			--SET @o_pl_error = 160034
			RAISERROR('Error ingresando en tabla tm_p2p_log', 16, 1) WITH NOWAIT
		END	
				
		--Validar cliente destino, primero se verifica si es SOCIO
		SELECT @w_ente_dest = en_ente,
			   @w_ente_mis_dest = en_ente_mis,
			   @w_cta_banco_dest = ps_cta_banco,
			   @w_producto_dest = ps_producto,
			   @w_nom_cliente_benef = ps_nombre
		FROM   cob_bvirtual..bv_ente
			INNER JOIN cob_atm..tm_p2p_socio
			ON en_ente_mis = ps_ente_mis
		WHERE  ps_telefono = @w_telefono_dest
		AND    ps_estado = 'A'

		IF @@ROWCOUNT > 0
		BEGIN
			SET @w_socio = 'S'

			--Si es socio no es factible realizar un C2P entrante
			IF @w_tipo_pago = 'C2P'
			BEGIN
				SET @o_pl_error = 160034
                RAISERROR('El cliente no puede realizar un C2P', 16, 1) WITH NOWAIT
			END		
		END
		ELSE
		BEGIN          	
			--Validar cliente destinatario segun el tipo de transaccion
			SELECT @w_tipo_cliente = CASE @w_op_code WHEN '560050' THEN 'C'
													ELSE 'P'
									END
				
			--	Validar datos del cliente
			SET @w_ente_dest = NULL
			SET @w_autorizado  = 'N'

			-- buscar el ente a quien hay que debitarle
			SELECT @w_ente_dest = en_ente
				,@w_ente_mis_dest = en_ente_mis
				,@w_nom_cliente_benef = en_nombre
				,@w_autorizado = en_autorizado
			FROM  cob_bvirtual..bv_ente a
			WHERE en_ced_ruc = @i_cedula_dest
			AND   en_tipo    = @w_tipo_cliente

			IF @@rowcount = 0
			BEGIN
				SET @o_pl_error = 160040
				RAISERROR('Cédula del receptor invalida verifique datos', 16, 1) WITH NOWAIT
			END			

			--Validar si el telefono corresponde al medio de envio del servicio 1
			IF @w_op_code = '560050' -- P2C	
				/*SELECT @w_ente_dest = b.en_ente
					,@w_ente_mis_dest = b.en_ente_mis
					,@w_nom_cliente_benef = b.en_nombre
					,@w_ente_adm = a.en_ente
				FROM cob_bvirtual..bv_medio_envio
					INNER JOIN cob_bvirtual..bv_ente a
					ON me_ente = a.en_ente
					INNER JOIN cob_bvirtual..bv_user_adm
					ON a.en_ente_mis = ua_ente_adm
					INNER JOIN cob_bvirtual..bv_ente b
					ON ua_ente_jur = b.en_ente_mis
				WHERE me_num_dir = '0' + right(@i_telefono_dest,10)
				AND   b.en_tipo = 'C'
				AND   b.en_ced_ruc = @i_cedula_dest
				AND   me_servicio = 1*/
				SELECT @w_ente_adm = a.en_ente
				FROM cob_bvirtual..bv_medio_envio
					INNER JOIN cob_bvirtual..bv_ente a
					ON me_ente = a.en_ente
					INNER JOIN cob_bvirtual..bv_user_adm
					ON a.en_ente_mis = ua_ente_adm
					AND ua_ente_jur = @w_ente_mis_dest
				WHERE me_num_dir = @w_telefono_dest
				AND   me_servicio = 1
			ELSE --P2P - C2P
				SELECT @w_ente_adm = me_ente
				FROM cob_bvirtual..bv_medio_envio
				WHERE me_ente = @w_ente_dest
				AND   me_num_dir = @w_telefono_dest
				AND   me_servicio = 1

			IF @@rowcount = 0 OR ISNULL(@w_ente_adm,0) = 0
			BEGIN
				SET @o_pl_error = 160038
				RAISERROR('No coincide el celular con el afiliado a la cedula', 16, 1) WITH NOWAIT
			END

			--- busco el alias de la cuenta principal del destino
			EXEC @w_return = cob_bvirtual..tic_sp_cons_cuentas_bv 
				@s_cliente = @w_ente_dest 				--codigo del cliente en BV
				,@s_servicio = @w_canal						--codigo del canal
				,@i_operacion = 'C'							--codigo indicador de la operacion
				,@o_producto = @w_producto_dest OUTPUT		--retorna el codigo del producto cobis
				,@o_cuenta = @w_cta_banco_dest OUTPUT 		--retorna el valor numero de cuenta-producto
		
			-- si no tiene cuenta principal utiliza la primera que se encuentre del cliente
			IF ISNULL(@w_cta_banco_dest,'') = ''
			BEGIN
				SELECT TOP 1 @w_producto_dest = ep_producto
						,@w_cta_banco_dest = ep_cuenta
				FROM  cob_bvirtual..bv_ente_servicio_producto
				WHERE ep_ente = @w_ente_dest
				AND   ep_producto IN (1, 3, 4)
				AND   ep_estado = 'V'
				AND   ep_moneda = 0
				AND   ep_cuenta_cobro = 'S'
				ORDER BY ep_producto DESC
			
				IF ISNULL(@w_cta_banco_dest,'') = ''
				BEGIN
					SET @o_pl_error = 169032
					RAISERROR('SERVICIO MIPAGO NO ASOCIADO AL CLIENTE', 16, 1) WITH NOWAIT								
				END
			END	

			--Afiliacion del cliente a P2P
			IF NOT EXISTS (SELECT  1
						FROM  cob_bvirtual..bv_p2p
						WHERE  pp_ente = @w_ente_dest)
			BEGIN
				SET @o_pl_error = 169032
				RAISERROR('SERVICIO MIPAGO NO ASOCIADO AL CLIENTE', 16, 1) WITH NOWAIT	
			END

			--Validar que este afiliado a Banca en Linea
			IF NOT EXISTS (SELECT  1
							FROM   cob_bvirtual..bv_login
							WHERE  lo_ente = @w_ente_dest
							AND    lo_servicio = 1
							AND    lo_autorizado = 'S') AND @w_autorizado = 'S'
			BEGIN
				SET @o_pl_error = 1875000
				RAISERROR('USTED DEBE AFILIARSE A BANCA EN LINEA', 16, 1) WITH NOWAIT							
			END						

			--Validar el cupo permitido para C2P sino es reverso 
			IF @w_corr <> 'R' AND @w_op_code = '560009' 
			BEGIN
				EXEC @w_return = cob_bvirtual..tic_sp_valida_cupo 
					@i_ssn_branch = @w_ssn_branch
					,@i_fecha_proc = @w_fecha_proc
					,@i_srv_local = @w_srv_local
					,@i_ente_mis_dest = @w_ente_mis_dest
					,@i_eq_srv_org = 'BVIRTUAL'
					,@i_eq_trn_org = 'PAGOC2P'
					,@i_canal = 1
					,@i_monto_val = @i_monto
					,@i_producto_dest = 3
					,@o_tarjeta = @w_tarjeta OUTPUT
					,@o_tipo_cupo = @w_tipo_cupo OUTPUT
					,@o_cupo_tdd = @w_cupo OUTPUT
					,@o_comision = @w_comision OUTPUT
					,@o_afecta_cupo = @w_afecta_cupo OUTPUT
					,@o_saldo_cupo_new = @w_saldo_cupo_new OUTPUT
					,@o_fecha_cupo_new = @w_fecha_cupo_new OUTPUT
					,@o_num_trans_new = @w_num_trans_new OUTPUT
					,@o_mensaje = @o_mensaje OUTPUT

				IF @w_return <> 0
				BEGIN
					SET @o_pl_error = 160033
					RAISERROR('Monto a pagar es inferior al minimo permitido', 16, 1) WITH NOWAIT	
				END
			END				
		END
		--Fin Validar si el Ente receptor es socio

		--Validar banco emisor
		SET @w_validar_banco = 0
		IF ISNUMERIC(@i_banco_org) = 1
		BEGIN
			SET @w_banco_pago = convert(INT, @i_banco_org)
			SET @w_banco_char = '0' + convert(CHAR(3), @i_banco_org)

			IF @w_banco_r4 = @w_banco_char
				SET @w_validar_banco = 1
			ELSE
			BEGIN
				SELECT @w_habilitado = c.codigo
				FROM   cobis..cl_tabla t
					INNER JOIN cobis..cl_catalogo c
					ON t.codigo = c.tabla
				WHERE t.tabla = 'cl_bancos_ptp'
				AND   c.estado = 'V'
				AND   c.codigo = @w_banco_char
				
				IF @@rowcount = 0
					SET @w_validar_banco = 1
			END
		END
		ELSE
			SET @w_validar_banco = 1

		--El banco origen no es correcto
		IF (@w_validar_banco = 1)
		BEGIN
			SET @o_pl_error = 160042
			RAISERROR('Banco receptor no afiliado', 16, 1) WITH NOWAIT	
		END

		IF @w_op_code = '560009' OR @w_op_code = '200030' -- C2P
		BEGIN	
			SET @w_param_mto_min = 'MINC2P'
			SET @w_param_mto_max = 'MAXC2P'

			-- Verificar monto minimo y maximo del pago c2p
			SELECT @w_monto_max = pa_money
			FROM  cobis..cl_parametro
			WHERE pa_producto = 'BV'
			AND   pa_nemonico = @w_param_mto_max 

			SELECT @w_monto_min = pa_money
			FROM  cobis..cl_parametro
			WHERE pa_producto = 'BV'
			AND   pa_nemonico = @w_param_mto_min	

			IF (@i_monto < @w_monto_min) OR (@i_monto > @w_monto_max) --and @i_ts='N'
			BEGIN
				SET @o_pl_error = 160033
				RAISERROR('Monto a pagar es inferior al minimo permitido', 16, 1) WITH NOWAIT	
			END	
			--Valida Montos por tipo de pago
		END

		--Buscar y validar Secuencial de reverso
		IF @w_op_code = '200030' --AND @w_corr = 'N'   --Anulacion
		BEGIN
			IF @w_corr = 'N'   --Anulacion
				SELECT @w_sec_reverso = sr_sec_reverso
				FROM   cob_atm..tm_secuencial_reverso
				WHERE  sr_bit37 = @i_telefono_org
				AND    substring(sr_clave_trn, 1, 1) != 'R'

			IF @w_corr = 'R'	--Reverso Anulacion
				SELECT @w_sec_reverso = sr_sec_reverso
				FROM  cob_atm..tm_secuencial_reverso
				WHERE sr_clave_trn = @i_trace
				and   sr_bit37 = @i_bit37	

			IF ISNULL(@w_sec_reverso,0) = 0
			BEGIN
				SET @o_pl_error = 160035
				RAISERROR('Error en formato', 16, 1) WITH NOWAIT
			END			
		END
		ELSE
		BEGIN
			SELECT @w_sec_reverso = sr_sec_reverso
			FROM   cob_atm..tm_secuencial_reverso
			WHERE  sr_clave_trn = @i_trace	
		END
		--Fin validar Secuencial de reverso

		IF (isnull(@w_sec_reverso,0) = 0 AND @w_corr = 'R') OR (isnull(@w_sec_reverso,0) <> 0 AND @w_corr = 'N')   --Reversos / Normales
		BEGIN
			SET @o_pl_error = 160035
			RAISERROR('Error en formato', 16, 1) WITH NOWAIT
		END	

		--Debe existir el registro en log por la validación de datos
		SELECT @w_result = pl_result
		FROM   cob_atm..tm_p2p_log
		WHERE  pl_trace = @i_trace
		AND    pl_referencia = @i_bit37
		AND    pl_corr = 'N'	

		IF (@@ROWCOUNT = 0) OR (ISNULL(@w_result,'') = '' AND @w_corr = 'R') --OR (ISNULL(@w_result,'') <> '' AND @w_corr = 'N')
		BEGIN
			SET @o_pl_error = 50004
			RAISERROR('NO EXISTE LA TRASACCION DE VERIFICACION PAGO ENTRANTE', 16, 1) WITH NOWAIT
		END

		--En caso de reverso buscar los numeros de la transaccion original - Cambio por reverso de Vuelto
		IF @w_corr = 'R' 
		BEGIN
			IF @w_result <> '00'
			BEGIN			
				SET @o_pl_error = 160035
				RAISERROR('Error en formato', 16, 1) WITH NOWAIT
			END

			-- buscar monto original , telefono y cedula 
			SELECT @w_monto = isnull(tm_valor, 0)
				,@i_telefono_org = '58' + right(tm_campo2,10)
				,@i_telefono_dest = '58' + right(tm_campo3,10)
				,@i_cedula_dest = tm_campo4
				,@w_signo = tm_signo
			FROM  cob_remesas..re_tran_monet
			WHERE tm_ssn_host = @w_sec_reverso	
			
			IF @w_op_code = '560000' AND @w_signo = 'D' --Reverso de Vuelto
				SEt @w_telefono_soc = @i_telefono_org
		END	

		--==== Verificar si los productos CC y AH estan habilitados
		SELECT @w_prod_habilitado_AH = pm_estado
			FROM cobis..cl_pro_moneda 
			WHERE pm_producto = 4
			AND pm_tipo = 'R'   

		SELECT @w_prod_habilitado_CC = pm_estado
			FROM cobis..cl_pro_moneda 
			WHERE pm_producto = 3
			AND pm_tipo = 'R'

		-- Si estan deshabilitados envia error 91
		IF ((isnull(@w_prod_habilitado_AH,'') <> 'V') or (isnull(@w_prod_habilitado_CC,'') <> 'V'))
		BEGIN
			SET @o_pl_error = 160041--40002 --160041
			RAISERROR('Banco receptor del pago no activo', 16, 1) WITH NOWAIT
		END

		SET @o_pl_error = 0
		SET @o_cta_dest = @w_cta_banco_dest

		IF ISNULL(@o_retorno,'') = ''
			SEt @o_retorno = '00' --'100'

		IF ISNULL(@o_mensaje,'') = ''
			SEt @o_mensaje = 'TRANSACCION EXITOSA'	
	END TRY
	BEGIN CATCH
		--Buscar mensaje asociado al error
		IF ISNULL(@o_pl_error,0) > 0
			SELECT @o_retorno = er_error_red ,
				   @o_mensaje = mensaje
			FROM   cob_atm..tm_errores_red
				INNER JOIN cobis..cl_errores
				ON er_error_cobis = numero
			AND   er_error_cobis = @o_pl_error
			AND   er_srv = 'P2P'
		ELSE
			SET @o_pl_error = 50004
  
		SET @o_retorno = ISNULL(@o_retorno,'12')
		SET @o_mensaje = ISNULL(@o_mensaje,'TRANSACCION INVALIDA')

		SELECT @w_fecha_log = MAX(pl_hora)
		FROM   cob_atm..tm_p2p_log
		WHERE pl_trace = @i_trace
		AND   pl_telefono_d = @i_telefono_dest
		AND   pl_referencia = @i_bit37
		AND   pl_result IS NULL
		AND   pl_corr = @w_corr
	
		UPDATE cob_atm..tm_p2p_log 
		SET    pl_hora2 = getdate(),
		       pl_ente_comercio = @w_ente_mis_dest,
			   pl_result = @o_retorno,
			   pl_error =  @o_pl_error,
			   pl_procesar = 'N'
		FROM   cob_atm..tm_p2p_log
		WHERE  pl_telefono_d = @i_telefono_dest
		AND    pl_trace = @i_trace
		AND    pl_referencia = @i_bit37
		AND    pl_result IS NULL
		AND    pl_corr = @w_corr
		AND    ISNULL(pl_procesar,'') IN ('','N')
		AND    pl_hora = @w_fecha_log
		/*
			SELECT MAX(pl_hora)
			FROM cob_atm..tm_p2p_log
			WHERE pl_telefono_d = @i_telefono_dest
			AND pl_trace = @i_trace
			AND pl_referencia = @i_bit37
			AND pl_result IS NULL
			AND pl_corr = @i_corr
			AND ISNULL(pl_procesar,'') IN ('','N')
		)*/

		--Actualizar informacion para reporte de pago telefonico
		-- @w_telefono_org = '58' + RIGHT(@i_telefono_org, 10)
		--SET @w_telefono_dest = '58' + RIGHT(@i_telefono_dest, 10)
		SET @w_banco_char = '0' + RIGHT(@i_banco_org,3)   
	
		EXEC @w_return = cob_bvirtual..sp_log_p2p 
			 @i_servicio = @w_tipo_pago
			,@i_telefono_o = @i_telefono_org
			,@i_telefono_d = @i_telefono_dest
			,@i_monto = @w_monto
			,@i_banco = @w_banco_char
			,@i_cta_banco = @w_cta_banco_dest
			,@i_reverso = @i_corr
			,@i_codigo_respuesta = @o_retorno
			,@i_error_cobis = 0
			,@i_msg = @o_mensaje
			,@i_referencia = @i_bit37
			,@i_tipo = @w_tipo
			,@i_ssn_branch = 0
			,@i_manejo_cola = 'N'
			,@i_signo = @w_signo
			,@i_secuencial = 0       
			
	END CATCH
END
RETURN 0
GO
SET ANSI_NULLS OFF
go
SET QUOTED_IDENTIFIER OFF
go
IF OBJECT_ID('dbo.atm_val_p2p_ws') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.atm_val_p2p_ws >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.atm_val_p2p_ws >>>'
go
