USE cob_atm
go
IF OBJECT_ID('dbo.atm_p2p_ws') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.atm_p2p_ws
    IF OBJECT_ID('dbo.atm_p2p_ws') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.atm_p2p_ws >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.atm_p2p_ws >>>'
END
go
SET ANSI_NULLS ON
go
SET QUOTED_IDENTIFIER ON
go
/************************************************************/
/*  Archivo:            atm_p2p_ws.sp                       */
/*  Stored procedure:   atm_p2p_ws                          */
/*  Producto:           ATM                                 */
/************************************************************/
/*                          PROPOSITO                       */
/* Ejecutar las transacciones de pago movil entrente        */
/************************************************************/
/*                        MODIFICACIONES                    */
/*  FECHA              AUTOR              RAZON             */
/*  24/10/2024         Jaykyec        EMISION INICIAL       */
/*  29/10/2024         Jaykyec    Enviar Nombre del Socio   */
/*                             para crear la Tran. Servicio */
/*  06/11/2024         Jayktec  C2P,Anulacion,Reverso       */
/*  24/01/2025         Jayktec  Retorno de codigo 12 cuando */
/*								return = 0 u error_cobis!= 0*/
/*  03/02/2025         Jayktec  Extraer manejo de errores   */
/************************************************************/

CREATE PROCEDURE [dbo].[atm_p2p_ws] (
	 @s_user            VARCHAR(30) = 'EntranteR4'
	,@i_srv_org			VARCHAR(30) = ''
	,@i_corr			CHAR(1)     = 'N'					--N=No, S=Si-Reverso
	,@i_telefono_org	VARCHAR(12) = NULL
	,@i_cedula_orig		VARCHAR(20) = NULL	
	,@i_banco_org		VARCHAR(16) = NULL
	,@i_telefono_dest	VARCHAR(12) = NULL
	,@i_cedula_dest		VARCHAR(20) = NULL
	,@i_op_code			VARCHAR(8)  = NULL
	,@i_bit37		    VARCHAR(12) = NULL				    --Referencia del Emisor (bit37)
	,@i_trace			VARCHAR(64) = NULL
	,@i_monto			MONEY       = NULL
	,@i_otp             VARCHAR(8) = NULL
	,@o_retorno			VARCHAR(3) = NULL OUTPUT
	,@o_pl_error		INT = NULL OUTPUT
	,@o_mensaje			VARCHAR(200) = NULL OUTPUT
	,@o_ssn_branch      INT        = NULL OUTPUT
	,@o_cta_dest		VARCHAR(24) = NULL OUTPUT
)
AS
BEGIN
	DECLARE  @w_ssn_branch    INT
			,@s_srv           VARCHAR(30)
			,@w_mensaje       VARCHAR(255)
			,@w_sec_reverso   INT
			,@w_oficina_org   SMALLINT
			,@w_prod_habilitado_AH CHAR(1)
			,@w_prod_habilitado_CC CHAR(1)
			,@w_banco_pago    INT
			,@w_srv_host      VARCHAR(30)
			,@w_srv_local     VARCHAR(30)
			,@w_telefono_org  VARCHAR(15)
			,@w_telefono_dest VARCHAR(15)
			,@w_bit37    	  INT				
			,@w_op_code 	  VARCHAR(8)
			,@w_corr 		  CHAR(1)
			,@w_tipo_pago 	  VARCHAR(3)
			,@w_return 		  INT
			,@w_monto		  MONEY
			,@w_banco_char    CHAR(4)
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
			,@w_causa           char(3)
			,@w_causa_com       char(3)
			,@w_habilitado      varchar(10)
			,@w_otp             numeric(8)
			,@w_hora		    datetime
			,@w_ing_tran        char(1)
			,@w_concepto        VARCHAR(80)
			,@w_saldo_conta     MONEY
			,@w_saldo_dispo     MONEY
			,@w_sec             INT
			,@w_ofi_cta_ori     INT
			,@w_result          CHAR(2)
			,@w_cta_orig        VARCHAR(20)
			,@w_signo           CHAR(1)
			,@w_tran_servicio   CHAR(1)
			,@w_est_notifica    VARCHAR(4)
			,@w_correo_cobra    VARCHAR(255)
			,@w_fecha_notif     VARCHAR(50)
			,@w_tipo            VARCHAR(10)
			,@w_manejo_cola     CHAR(1)
			,@w_cedula_dest     VARCHAR(20)
			,@w_fecha_log		DATETIME
			,@w_clave_trace		VARCHAR(64)
			,@w_telefono_soc	VARCHAR(12)
			,@w_validar_banco	TINYINT
			,@w_param_mto_min   VARCHAR(10)
			,@w_param_mto_max   VARCHAR(10)
			,@w_param_cau       VARCHAR(10)
			,@w_param_cau_com   VARCHAR(10)
			,@w_tipo_cliente    CHAR(1)
			,@w_procesar        CHAR(1)
			,@w_autorizado		CHAR(1)
			,@w_cobro_comision  CHAR(1)
			,@w_sybase          CHAR(1)
	
	BEGIN TRY
		--Inicializar variables de Trabajo
		SET @w_ing_tran = 'N'
		SET @w_socio = 'N'
		SET @w_manejo_cola = 'N'
		SEt @w_telefono_soc = @i_telefono_dest
		SET @w_ente_mis_dest = 0
		SET @w_bit37 = convert(INT, right(@i_bit37, 9))
		SET @i_op_code = left(@i_op_code, 6)
		SET @w_op_code = @i_op_code
		SET @s_srv = @i_srv_org
		SET @w_monto = convert(money, @i_monto)
		SET @w_canal = 4
		SET @w_tran_servicio = 'N'
		SET @w_telefono_org = '0' + right(@i_telefono_org,10)
		SET @w_telefono_dest = '0' + right(@i_telefono_dest,10)
		SET @w_return = -1
		SET @w_cobro_comision = 'N'
		SET @w_sybase = 'S'

		--Inicializar variables de Salida
		SET @o_retorno = NULL
		SET @o_pl_error = NULL
		SET @o_mensaje = NULL
		SET @o_ssn_branch = NULL
		SET @o_cta_dest = NULL

		SET @w_tipo = 'RECIBIDA'
		SET @w_signo = 'C'

		IF @i_op_code = '560009' 
		BEGIN
			SET @w_tipo = 'ENVIADA'		
			SET @w_signo = 'D'	
		END
		
		IF @i_corr = 'S'
			SET @w_corr = 'R'
		ELSE
			SET @w_corr = @i_corr

		--La anulacion no se maneja como reverso
		IF @w_corr = 'N' AND @w_op_code = '200030'
			SET @w_corr = 'N'

		--Oficina segun servidor
		SELECT @w_oficina_org = ro_oficina
		FROM   cob_atm..tm_redes_oficinas
		WHERE  ro_red = @i_srv_org

		-- Obtiene el nombre del servidor remoto
		SELECT @w_srv_host = pa_char
		FROM   cobis..cl_parametro
		WHERE  pa_nemonico = 'SRVR'
		AND    pa_producto = 'ADM'

		-- Obtiene el nombre del servidor local
		SELECT @w_srv_local = pa_char
		FROM   cobis..cl_parametro
		WHERE  pa_producto = 'BV'
		AND    pa_nemonico = 'BVSRV'	

		--Fecha de Proceso
		SELECT @w_fecha_proc = fp_fecha
		FROM   cobis..ba_fecha_proceso	
		
		--Tipo de pago recibido segun el codigo
		SET @w_tipo_pago = CASE 
								WHEN @w_op_code = '560000'
									THEN 'P2P'
								WHEN @w_op_code = '560009'
									THEN 'C2P'
								WHEN @w_op_code = '560050'
									THEN 'P2C'
								WHEN @w_op_code = '200030'    --JT-30102024 Anulacion
									THEN 'C2P'
							END

		--Definir parametros de causas contables
		SET @w_param_cau = 	CASE 
								WHEN @w_tipo_pago = 'P2P' THEN 'CAUP2P'
								WHEN @w_tipo_pago = 'P2C' THEN 'CAUP2C'
								WHEN @w_tipo_pago = 'C2P' THEN 'CAUC2P'
								ELSE 'CAUP2P'
							END

		SET @w_param_cau_com = 	CASE 
									WHEN @w_tipo_pago = 'P2P' THEN 'CACP2P'
									WHEN @w_tipo_pago = 'P2C' THEN 'CACP2C'
									WHEN @w_tipo_pago = 'C2P' THEN 'CACC2P'
									ELSE 'CACP2P'
								END

		--Causas contables
		SELECT @w_causa = pa_char
		FROM  cobis..cl_parametro
		WHERE pa_producto = 'ATM'
		AND   pa_nemonico = @w_param_cau 

		SELECT @w_causa_com = pa_char
		FROM  cobis..cl_parametro
		WHERE pa_producto = 'ATM'
		AND   pa_nemonico = @w_param_cau_com 

		--Consultar si el destino es cliente del banco con la identificacion envida
		SELECT @w_cedula_dest = en_ced_ruc
		FROM   cob_bvirtual..bv_ente
		WHERE  en_ced_ruc = @i_cedula_dest

		--Si no existe se eliminan los posibles 0 a la izquierda
		IF @w_cedula_dest IS NULL
			select @i_cedula_dest = convert(VARCHAR, convert(NUMERIC, cob_atm.dbo.numero(@i_cedula_dest)))

		IF (@w_corr = 'R' OR @w_op_code = '200030')--En caso de reverso guardar el registro del log
		BEGIN
			--Ingreso de informacion en el Log  
			INSERT INTO tm_p2p_log (
				pl_corr    ,pl_op_code ,pl_banco ,pl_telefono_o ,pl_telefono_d
				,pl_cedula ,pl_trace   ,pl_monto ,pl_tipo_pago  ,pl_referencia ,pl_procesar
				) 
			VALUES (
				@w_corr         ,@w_op_code ,@i_banco_org ,@i_telefono_org ,@i_telefono_dest
				,@i_cedula_dest ,@i_trace   ,@w_monto     ,@w_tipo_pago    ,@i_bit37 ,NULL) 

			IF @@ERROR > 0
				RAISERROR('ERROR INSERT', 16, 1) WITH NOWAIT
		END	

		--Buscar y validar Secuencial de reverso
		IF @w_op_code = '200030' --AND @w_corr = 'N'   --Anulacion
		BEGIN
			IF @w_corr = 'N'   --Anulacion
				SELECT @w_sec_reverso = sr_sec_reverso
						,@w_clave_trace = sr_clave_trace
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

		IF (isnull(@w_sec_reverso,0) = 0 AND @w_corr = 'R') OR (isnull(@w_sec_reverso,0) <> 0 AND @w_corr = 'N')   --Reversos / Normales
		BEGIN
			SET @o_pl_error = 160035 --30 ERROR DE FORMATO
			RAISERROR('Error en formato', 16, 1) WITH NOWAIT
		END	

	--En caso de reverso buscar los numeros de la transaccion original - Cambio por reverso de Vuelto
		IF @w_corr = 'R' 
		BEGIN
			IF @w_result <> '00'
			BEGIN			
				SET @o_pl_error = 160035 --30   Error ingresando secuencial de reverso
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

		--Debe existir el registro en log por la validación de datos
		SELECT @w_result = pl_result
		FROM   cob_atm..tm_p2p_log
		WHERE  pl_telefono_o 	= @i_telefono_org
		AND	   pl_telefono_d 	= @i_telefono_dest
		AND    pl_trace = @i_trace
		AND    pl_referencia = @i_bit37
		AND    pl_corr = 'N'	
		AND    pl_monto = @w_monto
		AND    pl_banco = @i_banco_org	
		ORDER BY pl_hora2 DESC

		IF (@@ROWCOUNT = 0) OR (ISNULL(@w_result,'') = '' AND @w_corr = 'R') --OR (ISNULL(@w_result,'') <> '' AND @w_corr = 'N')
		BEGIN
			SET @o_pl_error = 50004 --NO EXISTE LA TRASACCION DE VERIFICACION
			RAISERROR('NO EXISTE LA TRASACCION DE VERIFICACION 1', 16, 1) WITH NOWAIT
		END

		--Busca el ssn_branch que se refleja en la tran_monet
		EXEC cob_atm..sp_cseqnos 
			@i_tabla = 'tm_compra_pos'
			,@o_siguiente = @w_ssn_branch OUT

		SET @o_ssn_branch = @w_ssn_branch
		
		--Validar si el destino es un socio
		SELECT @w_ente_dest = en_ente,
			@w_ente_mis_dest = en_ente_mis,
			@w_cta_banco_dest = ps_cta_banco,
			@w_producto_dest = ps_producto,
			@w_tran_servicio = ps_tran_servicio,
			@w_est_notifica = ps_estado,
			@w_nom_cliente_benef = ps_nombre
		FROM   cob_bvirtual..bv_ente
			INNER JOIN cob_atm..tm_p2p_socio
			ON en_ente_mis = ps_ente_mis
		WHERE  ps_telefono = @w_telefono_dest
		AND    ps_estado = 'A'

		IF @@ROWCOUNT > 0
		BEGIN
			SET @w_socio = 'S'

			--Correo del socio
			SELECT @w_correo_cobra = me_num_dir
			FROM   cob_bvirtual..bv_medio_envio
			WHERE  me_tipo = 'MAIL'
			AND    me_default = 'S'
			AND    me_ente = @w_ente_dest

			--Si es socio no es factible realizar un C2P entrante
			IF @w_tipo_pago = 'C2P'
			BEGIN
				SET @o_pl_error = 160034	--TRANSACCION NO PERMITIDA
				RAISERROR('El cliente no puede realizar un C2P', 16, 1) WITH NOWAIT
			END			
		END

		--Validar si el Ente receptor es socio
		if @w_socio = 'N' --************************************* No Es Socio
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
				SET @o_pl_error = 160040 -- cedula no existe en cobis
				RAISERROR('Cédula del receptor invalida verifique datos', 16, 1) WITH NOWAIT
			END			

			--Validar si el telefono corresponde al medio de envio del servicio 1
			IF @w_op_code = '560050' -- P2C	
				SELECT @w_ente_adm = a.en_ente
				FROM cob_bvirtual..bv_medio_envio
					INNER JOIN cob_bvirtual..bv_ente a
					ON me_ente = a.en_ente
					INNER JOIN cob_bvirtual..bv_user_adm
					ON a.en_ente_mis = ua_ente_adm
					AND ua_ente_jur = @w_ente_mis_dest
				WHERE me_num_dir = @w_telefono_dest
				AND   me_servicio = 1			
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
				WHERE me_num_dir = '0' + right(@i_telefono_dest,10) --(Tlf Administrador)
				AND   b.en_tipo = 'C'
				AND   b.en_ced_ruc = @i_cedula_dest --(Rif compañia)
				AND   me_servicio = 1*/
			ELSE --P2P - C2P
				SELECT @w_ente_adm = me_ente
				FROM cob_bvirtual..bv_medio_envio
				WHERE me_ente = @w_ente_dest
				AND   me_num_dir = @w_telefono_dest
				AND   me_servicio = 1

			IF @@rowcount = 0
			BEGIN
				SET @o_pl_error = 160038 -- el telefono no se encuentra en cobis 
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
					SET @o_pl_error = 169032---71
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
			
			--Informacion para enviar corre
			SELECT @w_correo_cobra = me_num_dir
			FROM   cob_bvirtual..bv_medio_envio
			WHERE  me_tipo = 'MAIL'
			AND    me_default = 'S'
			AND    me_ente = @w_ente_adm
			
			--validar token 
			IF @w_corr <> 'R' AND @w_op_code = '560009'   -- C2P  @w_op_code = '560009' OR @w_op_code = '200030'
			BEGIN
				IF ISNULL(@i_otp,'') <> ''
				BEGIN
					SET @w_otp = right('00000000' + @i_otp, 8) 

					EXEC @w_return = cob_bvirtual..sp_genera_token_cw_bv
						@s_cliente    = @w_ente_dest,
						@s_servicio   = 1,
						@s_date       = @w_fecha_proc,
						@i_from       = 'P',					
						@i_operacion  = 'P',
						@i_login      = NULL,
						@i_token_recibido = @w_otp
			
					IF @w_return <> 0 --Si retorna 0 la OTP es valida
					BEGIN
						SET @o_pl_error = 169026 --55 Token Invalido 
						RAISERROR('OTP invalido', 16, 1) WITH NOWAIT
					END
				END
				ELSE
				BEGIN
					SET @o_pl_error = 169026 --55 Token Invalido 
					RAISERROR('OTP invalido', 16, 1) WITH NOWAIT
				END	
				--Validar el cupo permitido para C2P sino es reverso o anulacion
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
					SET @o_pl_error = 160033 --13 -CUPO INSUFICIENTE
					RAISERROR('Monto a pagar es inferior al minimo permitido', 16, 1) WITH NOWAIT	
				END
			END --Fin validar C2P
			
		END -- Fin Validar ente	no socio	

		--Validar banco emisor
		SET @w_validar_banco = 0
		IF ISNUMERIC(@i_banco_org) = 1
		BEGIN
			SET @w_banco_pago = convert(INT, @i_banco_org)
			SET @w_banco_char = '0' + convert(CHAR(3), @i_banco_org)
			
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
			SET @w_signo = 'D' --Debito
			SEt @w_tipo = 'ENVIADA'
			
			IF @w_op_code = '200030' and @i_corr = 'N' --Anulacion --JT-30102024
			begin
				SET @w_signo = 'C' --Credito
				SEt @w_tipo = 'RECIBIDA'		
			END

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
				RAISERROR('Monto a pagar no esta en el rango permitido', 16, 1) WITH NOWAIT	
			END	
			--Valida Montos por tipo de pago
		END		

		--Hora del registro en la tran_monet de remesas
		SET @w_hora = getdate()
		SET @w_concepto = 'DE:' + substring(@w_telefono_org, 3, 9) + ' REF:' + substring(@w_telefono_dest, 3, 9)
		SET @w_cta_orig = right('0000' + isnull(@i_banco_org, ''), 4) + isnull(@w_telefono_org, '')
		
		IF @w_corr = 'N' --Solo se ingresa la transaccion cuando no es reverso
		BEGIN
			SET @w_ing_tran = 'S'

			--EJECUTAR TRANSACCION DE servicio
			EXEC @w_return = cob_bvirtual..tic_sp_ins_tran_monet 
					@s_date = @w_fecha_proc
					,@s_srv_local = @w_srv_local
					,@s_user = 'cobisdatos'
					,@s_term = 'consola'
					,@t_corr = 'N'
					,@t_ssn_corr = @w_sec_reverso
					,@t_aut = 'N'
					,@t_login_aut = NULL
					,@s_rol = 1
					,@s_servicio = @w_canal
					,@t_trn = 2809
					,@s_ofi = 991
					,@t_filial = 1
					,@i_operacion = 'I'
					,@i_cuenta = @w_cta_banco_dest
					,@i_moneda = 0
					,@i_producto = @w_producto_dest
					,@i_cta_des = @w_cta_banco_dest
					,@i_concepto = @w_concepto
					,@i_prod_des = @w_producto_dest
					,@i_valor = @w_monto
					,@i_signo = @w_signo
					,@i_nom_cliente_benef = @w_nom_cliente_benef 
					,@s_ssn_branch = @w_ssn_branch
					,@i_hora = @w_hora
					,@i_srv_host = @w_srv_host 
					,@i_estado_ejec = 'EV'
					,@i_tipo_ejec = 'L'
					,@i_ofi_cta = @w_oficina_org
					,@i_tel_ori = @w_telefono_org
					,@i_tel_des = @w_telefono_dest
					,@i_ced_ruc = @i_cedula_dest

			IF @w_return <> 0
			BEGIN
				SET @o_pl_error = @w_return
				RAISERROR('Error ingresando transaccion monetaria local', 16, 1) WITH NOWAIT
			END	
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
			SET @o_pl_error = 160041  --91
			RAISERROR('Banco receptor del pago no activo', 16, 1) WITH NOWAIT
		END
		--====== Fin verificar producto 	

		--EJECUTAR TRANSACCION MONETARIA
		SET @w_return = NULL

		SET @i_telefono_org = ISNULL(@i_telefono_org,'')
		
		EXEC (
			'?=exec cob_cuentas..sp_atm_pago_ws
				?,?,?,?,?,
				?,?,?,?,?,
				?,?,?,?,?,
				?,?,?,?,?,
				?,?,?,?,?,
				?,?,?,?,?,
				?,?,?,?,?,
				?,?,?,?,?,
				?,?,?,?,?,
				?,?,?,?'
				,@w_return OUTPUT

				,0									--s_ssn  --secuencial tran monet
				,@w_srv_local						--s_srv
				,@w_srv_host						--lsrv
				,@s_user     						--s_user
				,0									--s_sesn

				,'consola'							--s_term
				,@w_fecha_proc						--s_date
				,991        						--s_ofi --991
				,1									--s_rol
				,''									--s_orig_err

				,0									--s_error
				,0									--s_sev				
				,''									--s_msg
				,''									--s_org
				,@w_corr							--t_corr

				,''									--t_ejec
				,@w_sec_reverso						--t_ssn_corr
				,'N'								--t_debut
				,''									--t_file
				,''									--t_from

				,'N'								--t_rty
				,0									--t_trn
				,'I'								--i_operacion
				,@w_ente_mis_dest					--i_cliente
				,@w_producto_dest					--i_producto

				,@w_cta_banco_dest					--i_cta_banco
				,@w_monto							--i_val
				,@w_causa							--i_causa
				,@w_causa_com						--i_causa_com
				,@w_banco_pago						--i_banco

				,@w_bit37					        --i_referencia
				,@w_concepto						--i_concepto
				,@w_ssn_branch						--i_ssn_branch
				,@w_tran_servicio  					--i_tran_servicio
				,@w_socio							--i_socio

				,@i_trace							--i_trace
				,@w_op_code							--i_op_code
				,@w_cta_orig                        --i_cta_orig
				,@i_cedula_orig	                    --i_cedula_org
				,@w_telefono_org					--i_telefono_org

				,@i_cedula_dest	                    --i_cedula_dest
				,@w_telefono_dest					--i_telefono_dest,
				,@w_nom_cliente_benef               --i_nom_cliente_benef
				,@w_signo							--i_signo
				,@w_sec            OUTPUT 
				
				,@w_saldo_dispo    OUTPUT 			
				,@w_saldo_conta    OUTPUT  
				,@w_ofi_cta_ori    OUTPUT
				,@w_manejo_cola    OUTPUT

			) at SYBATMLINK

		SET @w_sybase = 'S'

		if isnull( @w_return,1) <> 0
		BEGIN
			SET @o_pl_error = @w_return
			RAISERROR('Error procesando pago', 16, 1) WITH NOWAIT
		END
		
		IF @w_corr = 'N' --Solo se actualiza la transaccion cuando no es reverso
		BEGIN
			INSERT INTO cob_atm..tm_secuencial_reverso
			VALUES ( @i_trace
					,@i_trace
					,@w_sec
					,@w_oficina_org
					,@i_bit37 )
				
			--Ingresa la dispersion para los clientes con este servicio
			IF EXISTS (
					SELECT 1
					FROM cob_remesas..maestro_dispersion
					WHERE md_cuenta = @w_cta_banco_dest
						AND md_tipo = 'PAGO MOVIL'
						AND md_estado = 'A'
					)
			BEGIN
				EXEC @w_return = cob_remesas..sp_valida_maestro 
					@i_telefono_o = @w_telefono_org
					,@i_telefono_d = @w_telefono_dest
					,@i_monto = @i_monto
					,@i_banco = @i_banco_org
					,@i_cta_banco = @w_cta_banco_dest
					,@i_referencia = @w_bit37
					,@i_concepto = @w_concepto
					,@i_modo = 1
			END 			
		
			-- ACTUALIZAR CUPO DE LA TRANSACCION MONETARIA EN LA TDD ---
			IF @w_op_code = '560009' --OR @i_op_code LIKE '%200030%'   --JT-30102024
			BEGIN
				EXEC @w_return = cob_atm..sp_actualiza_tarjeta_atm 
					@t_idioma = 1
					,@s_ssn_branch = @w_ssn_branch
					,@i_num_tran = @w_num_trans_new
					,@i_oficina_org = @w_oficina_org
					,@i_estado_ejec = 'EJ'
					,@i_tarjeta = @w_tarjeta
					,@i_afecta_cupo = @w_afecta_cupo
					,@i_tipo_cupo = @w_tipo_cupo
					,@i_cupo = @w_cupo
					,@i_saldo_cupo = @w_saldo_cupo_new
					,@i_fecha_cupo = @w_fecha_cupo_new
					,@i_host_central = 'N'
					,@i_moneda = 0
					,@i_factor = 0 ------@w_factor
			END

			--Notificacion al destinatario si no es socio o los socios pero sin utilizar la cola de notas de credito
			IF @w_socio = 'N' --OR @w_manejo_cola = 'N'
			BEGIN
				SET @w_fecha_notif = CONVERT(VARCHAR(10),GETDATE(),103) + ' ' + ltrim(rtrim(substring(convert(VARCHAR(10), GETDATE(), 108), 1, 8)))

				EXEC @w_return = cob_bvirtual..sp_envia_sms 
					@t_trn = 1845
					,@i_telefono = @i_telefono_org --@w_celular
					,@i_operacion = 'S'
					,@i_texto_corto = '18799'
					,@i_aux1 = @w_telefono_org--@w_aux1
					,@i_aux2 = @w_fecha_notif
					,@i_aux3 = @w_monto
					,@i_aux4 = @w_bit37 --@w_aux4
					,@i_aux5 = @i_cedula_dest --@w_cedula_benf
					,@i_monto = @w_monto

				/* enviar correo al que cobra */
				IF @w_correo_cobra IS NOT NULL
				BEGIN
					EXEC @w_return = cob_bvirtual..tic_sp_cons_mensaje_bv
						@i_operacion = 'S'
						,@i_valor_texto = '18799'
						,@i_aux1 = @i_telefono_org--@w_aux1
						,@i_aux2 = @w_fecha_notif
						,@i_aux3 = @w_monto
						,@i_aux4 = @w_bit37 --@w_aux4
						,@i_aux5 = @i_cedula_dest --@w_cedula_benf
						,@i_aux6 = '@i_aux6'
						,@o_texto_largo = @w_mensaje OUT

					EXEC @w_return = msdb..sp_send_dbmail 
						@recipients = @w_correo_cobra
						,@body = @w_mensaje
						,@body_format = 'HTML'
						,@subject = 'R4 Movil'
						,@exclude_query_output = 1
				END
			END
		END	
		ELSE -- Si es reverso y todo salio bien , eliminar la traza para evitar un doble reverso
		BEGIN
			IF @w_op_code = '200030'
				UPDATE cob_atm..tm_secuencial_reverso
				SET sr_clave_trn = 'R' + sr_clave_trn
				WHERE (	sr_bit37 = @i_bit37
				OR      sr_bit37 = @i_telefono_org	)
			ELSE
				UPDATE cob_atm..tm_secuencial_reverso
				SET sr_clave_trn = 'R' + sr_clave_trn
				WHERE sr_clave_trn = @i_trace

			-- si es reverso marcar la transaccion 
			UPDATE cob_remesas..re_tran_monet WITH (ROWLOCK)
			SET tm_estado_correccion = 'R'
			WHERE tm_ssn_host = @w_sec_reverso
		END

		--Solo procesa los pagos despues si es con manejo de cola (Socios)
		SET @w_procesar = 'N'
		SET @o_pl_error = 0
		SET @o_retorno = '00'
		SET @o_mensaje = 'TRANSACCION EXITOSA'	

		IF @w_manejo_cola = 'S'
			SET @w_procesar = NULL

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
			   pl_procesar  = @w_procesar
		FROM   cob_atm..tm_p2p_log
		WHERE  pl_telefono_d = @i_telefono_dest
		AND    pl_trace = @i_trace
		AND    pl_referencia = @i_bit37
		AND    pl_result IS NULL
		AND    pl_corr = @w_corr
		AND    ISNULL(pl_procesar,'') IN ('','N')
		AND    pl_hora = @w_fecha_log
		
		/*(
			SELECT MAX(pl_hora)
			FROM cob_atm..tm_p2p_log
			WHERE pl_telefono_d = @i_telefono_dest
			AND pl_trace = @i_trace
			AND pl_referencia = @i_bit37
			AND pl_result IS NULL
			AND pl_corr = @i_corr
			AND ISNULL(pl_procesar,'') IN ('','N')
		)*/

		IF @w_corr = 'N' 
		BEGIN
			UPDATE cob_remesas..re_tran_monet
			SET    tm_estado_ejecucion = 'EJ'
					,tm_control = @o_pl_error
					,tm_ssn_host = @w_sec
					,tm_saldo_contable = @w_saldo_dispo
					,tm_saldo_disponible = @w_saldo_conta
			WHERE tm_ssn_local = @w_ssn_branch	
			AND   tm_filial = 1   

			--Actualizar estadisticas
			EXEC cob_bvirtual..sp_p2p_est 
					@i_banco = @w_banco_char
				,@i_dir   = 'I'
				,@i_exito = 'S'
				,@i_monto = @w_monto	 
		END

		--SET @w_telefono_org = '58' + RIGHT(@i_telefono_org, 10)
		--SET @w_telefono_dest = '58' + RIGHT(@i_telefono_dest, 10)
/*		SET @w_banco_char = '0' + RIGHT(@i_banco_org,3) 

		EXEC @w_return = cob_bvirtual..sp_log_p2p 
			 @i_servicio = @w_tipo_pago
			,@i_telefono_o = @w_telefono_org
			,@i_telefono_d = @w_telefono_dest
			,@i_monto = @w_monto
			,@i_banco = @w_banco_char
			,@i_cta_banco = @w_cta_banco_dest
			,@i_reverso = @i_corr
			,@i_codigo_respuesta = @o_retorno
			,@i_error_cobis = @o_pl_error
			,@i_msg = @o_mensaje
			,@i_referencia = @i_bit37
			,@i_tipo = @w_tipo
			,@i_ssn_branch = @w_ssn_branch
			,@i_manejo_cola = @w_manejo_cola
			,@i_signo = @w_signo
			,@i_secuencial = @w_sec       
			*/
		--Cuenta destino
		SET @o_cta_dest = @w_cta_banco_dest
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
		SET     pl_hora2 = getdate(),
		        pl_ente_comercio = @w_ente_mis_dest,
				pl_result = @o_retorno,
				pl_error =  @o_pl_error,
				pl_procesar  = 'N'
		FROM   cob_atm..tm_p2p_log
		WHERE  pl_telefono_d = @i_telefono_dest
		AND    pl_trace = @i_trace
		AND    pl_referencia = @i_bit37
		AND    pl_result IS NULL
		AND    pl_corr = @w_corr
		AND    ISNULL(pl_procesar,'') IN ('','N')
		AND    pl_hora = @w_fecha_log
		/*(
			SELECT MAX(pl_hora)
			FROM cob_atm..tm_p2p_log
			WHERE pl_telefono_d = @i_telefono_dest
			AND pl_trace = @i_trace
			AND pl_referencia = @i_bit37
			AND pl_result IS NULL
			AND pl_corr = @i_corr
			AND ISNULL(pl_procesar,'') IN ('','N')
		)*/

		IF @w_ing_tran = 'S' AND @w_corr = 'N' 
		BEGIN
			UPDATE cob_remesas..re_tran_monet
			SET    tm_estado_ejecucion = 'EE'
					,tm_control = @o_pl_error
					,tm_ssn_host = @w_sec
					,tm_saldo_contable = @w_saldo_dispo
					,tm_saldo_disponible = @w_saldo_conta
			WHERE tm_ssn_local = @w_ssn_branch	
			AND   tm_filial = 1   

			--Actualizar estadisticas
			EXEC cob_bvirtual..sp_p2p_est 
				@i_banco = @w_banco_char
				,@i_dir   = 'I'
				,@i_exito = 'N'
				,@i_monto = @w_monto	 
		END

		IF @w_sybase = 'N' 
		BEGIN
			--SET @w_telefono_org = '58' + RIGHT(@i_telefono_org, 10)
			--SET @w_telefono_dest = '58' + RIGHT(@i_telefono_dest, 10)
			SET @w_banco_char = '0' + RIGHT(@i_banco_org,3) 
		
			EXEC @w_return = cob_bvirtual..sp_log_p2p 
				@i_servicio = @w_tipo_pago
				,@i_telefono_o = @w_telefono_org
				,@i_telefono_d = @w_telefono_dest
				,@i_monto = @w_monto
				,@i_banco = @w_banco_char
				,@i_cta_banco = @w_cta_banco_dest
				,@i_reverso = @i_corr
				,@i_codigo_respuesta = @o_retorno
				,@i_error_cobis = 0
				,@i_msg = @o_mensaje
				,@i_referencia = @i_bit37
				,@i_tipo = @w_tipo
				,@i_ssn_branch = @w_ssn_branch
				,@i_manejo_cola = @w_manejo_cola
				,@i_signo = @w_signo
				,@i_secuencial = @w_sec       
		END		
	END CATCH
END

RETURN 0
go
SET ANSI_NULLS OFF
go
SET QUOTED_IDENTIFIER OFF
go
IF OBJECT_ID('dbo.atm_p2p_ws') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.atm_p2p_ws >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.atm_p2p_ws >>>'
go
