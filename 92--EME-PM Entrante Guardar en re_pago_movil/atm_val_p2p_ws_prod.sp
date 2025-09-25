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
	,@i_otp             NUMERIC(8) = NULL
	,@o_retorno			VARCHAR(2) = NULL OUTPUT
	,@o_mensaje			VARCHAR(200) = NULL OUTPUT
	,@o_cta_dest		VARCHAR(24) = NULL OUTPUT
)
AS
BEGIN
	DECLARE  @o_sld_girar     MONEY
			,@w_ssn_host      INT
			,@w_num_error     INT
			,@w_ssn_branch    INT
			,@s_srv           VARCHAR(30)
			,@w_status        VARCHAR(15)
			,@w_referemcia    INT
			,@w_mensaje       VARCHAR(255)
			,@w_sec_reverso   INT
			,@w_clave_trace   VARCHAR(64)
			,@w_oficina_org   SMALLINT
			,@w_prod_habilitado_AH CHAR(1)
			,@w_prod_habilitado_CC CHAR(1)
			,@w_banco_pago    INT
			,@w_banco         CHAR(3)
			,@w_srv_host      VARCHAR(30)
			,@w_srv_local     VARCHAR(30)
			,@w_bit37    	  INT				
			,@w_op_code 	  VARCHAR(8)
			,@w_corr 		  CHAR(1)
			,@w_tipo_pago 	  VARCHAR(3)
			,@w_return 		  INT
			,@w_monto		  MONEY
			,@w_banco_char    CHAR(3)
			,@w_ente_orig     INT
			,@w_monto_min	  MONEY
			,@w_monto_max	  MONEY
			,@w_fecha_proc    DATETIME
			,@w_ente_dest     INT
			,@w_ente_mis_dest INT
			,@w_ente_adm	  INT
			,@w_celular2      VARCHAR(20)
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
			,@w_retorno			char(2)
			,@w_habilitado      int
			,@w_otp             numeric(8)
			,@w_hora		    datetime
			,@w_ing_tran        char(1)
			,@w_concepto        VARCHAR(80)
			,@w_saldo_conta     MONEY
			,@w_saldo_dispo     MONEY
			,@w_sec             INT
			,@w_ofi_cta_ori     INT
			,@w_result          CHAR(2)
			,@w_cta_orig        VARCHAR(10)
			,@w_telefono_org	VARCHAR(12)
			,@w_telefono_dest	VARCHAR(12)
			,@w_cedula_dest     VARCHAR(20)
			,@w_fecha_log		DATETIME
			
	IF @i_corr = 'S'
		SET @w_corr = 'R'
	ELSE
		SET @w_corr = @i_corr

	SET @w_ing_tran = 'N'
	SET @w_socio = 'N'
	SET @w_ente_mis_dest = 0
	SET @w_bit37 = convert(INT, right(@i_bit37, 9))
	SET @i_op_code = left(@i_op_code, 6)
	SET @w_op_code = @i_op_code
	SET @s_srv = @i_srv_org
	SET @w_monto = convert(money, @i_monto)
	SET @w_canal = 4
	SET @w_causa = CASE 
						WHEN @i_op_code IN ('560009','200030') THEN '797'
						ELSE '777'
					END
	SET @w_causa_com = '779'
	SET @w_telefono_org = '0' + right(@i_telefono_org,10)
	SET @w_telefono_dest = '0' + right(@i_telefono_dest,10)

	--Validar banco emisor
	IF ISNUMERIC(@i_banco_org) = 1
	BEGIN
		SET @w_banco_pago = CONVERT(INT, @i_banco_org)
		SET @w_banco_char = CONVERT(CHAR(4),@w_banco_pago)
	END
	ELSE
		SET @w_banco_char = ''

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

	SET @w_tipo_pago = CASE 
						WHEN @i_op_code = '560000'
							THEN 'P2P'
						WHEN @i_op_code = '560009'
							THEN 'C2P'
						WHEN @i_op_code = '560050'
							THEN 'P2C'
						END

	--Consultar si el destino es cliente del banco con la identificacion envida
	SELECT @w_cedula_dest = en_ced_ruc
	FROM   cob_bvirtual..bv_ente
	WHERE  en_ced_ruc = @i_cedula_dest

	--Si no existe se eliminan los posibles 0 a la izquierda
	IF @w_cedula_dest IS NULL
		select @i_cedula_dest = convert(VARCHAR, convert(NUMERIC, cob_atm.dbo.numero(@i_cedula_dest)))

	--Validar si el destino es un socio
	SELECT @w_ente_orig = en_ente,
	       @w_ente_mis_dest = en_ente_mis,
		   @w_cta_banco_dest = ps_cta_banco,
		   @w_producto_dest = ps_producto
	FROM   cob_bvirtual..bv_ente
		INNER JOIN cob_atm..tm_p2p_socio
		ON en_ente_mis = ps_ente_mis
	WHERE  ps_telefono = @w_telefono_dest

	IF @@ROWCOUNT > 0
		SET @w_socio = 'S'

	--Ingreso de informacion en el Log  
	BEGIN TRY
		INSERT INTO tm_p2p_log (
			pl_corr
			,pl_op_code
			,pl_banco
			,pl_telefono_o
			,pl_telefono_d
			,pl_cedula
			,pl_trace
			,pl_monto
			,pl_tipo_pago
			,pl_ente_comercio
			,pl_referencia 
			,pl_procesar
			)
		VALUES (
			@w_corr
			,@i_op_code
			,@i_banco_org
			,@i_telefono_org
			,@i_telefono_dest
			,@i_cedula_dest
			,@i_trace
			,@w_monto
			,@w_tipo_pago
			,@w_ente_mis_dest
			,@i_bit37
			,NULL) 
	END TRY
	BEGIN CATCH
		SET @w_num_error = 50001 --ERROR INGRESO EN LOG PAGO

		GOTO ATM_ERROR   
	END CATCH
	
	--Si es socio no es factible realizar un C2P entrante
	IF ISNULL(@w_socio,'N') = 'S' AND @w_tipo_pago = 'C2P'
	BEGIN
		SET @w_num_error = 160034 --BANCO EMISOR NO AFILIADO O ACTIVO

		GOTO ATM_ERROR    	
	END	
	
	--Validar banco emisor
	IF ISNULL(@w_banco_char,'') != ''
	BEGIN
		--SET @w_banco_pago = convert(INT, @i_banco_org)
		--SET @w_banco_char = convert(CHAR(3), @w_banco_pago)
		
		SELECT @w_habilitado = count(1)
		FROM   cobis..cl_tabla t
			INNER JOIN cobis..cl_catalogo c
			ON t.codigo = c.tabla
		WHERE t.tabla = 'cl_bancos_ptp'
		AND   c.estado = 'V'
		AND   c.codigo = '0' + right(@i_banco_org,3)
		
		IF @@rowcount = 0
		BEGIN
			SET @w_num_error = 160042 --BANCO EMISOR NO AFILIADO O ACTIVO

			GOTO ATM_ERROR    
		END			
	END
	ELSE
	BEGIN
		SET @w_num_error = 160042 --BANCO EMISOR NO AFILIADO O ACTIVO

		GOTO ATM_ERROR    
	END	

	--==== Verificar si los productos CC y AH estan habilitados
	BEGIN TRY
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
			SET @w_num_error = 160041  --91
			
			GOTO ATM_ERROR
		END
	END TRY
	BEGIN CATCH
			SET @w_num_error = 160041 --91
			
			GOTO ATM_ERROR
	END CATCH
	--====== Fin verificar producto 
	
	IF @i_op_code = '200030' AND @w_corr = 'N'
	BEGIN
		SELECT @w_sec_reverso = sr_sec_reverso
				,@w_clave_trace = sr_clave_trace
		FROM   cob_atm..tm_secuencial_reverso
		WHERE  sr_bit37 = @w_telefono_org
		AND    substring(sr_clave_trn, 1, 1) != 'R'

		SET @w_corr = 'R'
		SET @i_op_code = '560009'

		IF isnull(@w_sec_reverso,0) = 0
		BEGIN
			SET @w_num_error = 50007 --30 160036

			GOTO ATM_ERROR    
		END
	/*
		IF NOT EXISTS (SELECT 1
						FROM   cob_atm..tm_secuencial_reverso
						WHERE  sr_clave_trn = @i_trace) AND isnull(@w_sec_reverso,'') = ''
		BEGIN
			INSERT INTO cob_atm..tm_secuencial_reverso
			VALUES (@i_trace,@i_trace,,@w_sec_reverso,'0',@i_bit37)
			
			if @@error != 0
			BEGIN
				SET @w_num_error = 160035 --30 160036

				GOTO ATM_ERROR  
			END
		END*/
	END
	ELSE
	BEGIN
		IF @i_op_code = '200030' AND @w_corr = 'R'
		BEGIN
			SELECT @w_sec_reverso = sr_sec_reverso
					,@w_clave_trace = sr_clave_trace
			FROM  cob_atm..tm_secuencial_reverso
			WHERE substring(sr_clave_trn, 1, 1) = 'R'
				AND sr_clave_trace = @i_trace

			SET @w_corr = 'N'
			SET @i_op_code = '560009'

			IF isnull(@w_sec_reverso,0) = 0
			BEGIN
				SET @w_num_error = 50007 --30 

				GOTO ATM_ERROR
			END

			-- buscar monto original , telefono y cedula 
			SELECT @w_monto = isnull(tm_valor, 0)
					,@i_telefono_dest = tm_campo3
					,@i_banco_org = substring(tm_campo2, 1, 3)
					,@i_telefono_org = substring(tm_campo2, 4, 12)
					,@i_cedula_dest = tm_campo4
			FROM  cob_remesas..re_tran_monet
			WHERE tm_ssn_local = @w_sec_reverso
		END
		ELSE
			SELECT @w_sec_reverso = sr_sec_reverso
					,@w_clave_trace = sr_clave_trace
			FROM   cob_atm..tm_secuencial_reverso
			WHERE  sr_clave_trn = @i_trace
	END

	IF isnull(@w_sec_reverso,0) = 0 AND @w_corr = 'R'
	BEGIN
		SET @w_num_error = 50007 --30 

		GOTO ATM_ERROR
	END
	
	--En caso de reverso busco el monto original de la transaccion
	IF @w_corr = 'R'
	BEGIN
		IF EXISTS (SELECT 1
					FROM   cob_atm..tm_p2p_log 
					WHERE  pl_telefono_o = @i_telefono_org
					AND    pl_telefono_d = @i_telefono_dest
					AND    pl_trace = @i_trace
					AND    pl_banco = @i_banco_org
					and    pl_result = '00')
		BEGIN
			SELECT @w_monto = isnull(tm_valor, 0)
			FROM   cob_remesas..re_tran_monet
			WHERE  tm_ssn_local = @w_sec_reverso
			AND    tm_estado_ejecucion = 'EJ'
		
			IF @@ROWCOUNT = 0
			BEGIN  
				SET @w_num_error = 50007 --30   Error ingresando secuencial de reverso

				GOTO ATM_ERROR
			END	
		END
		ELSE
		BEGIN
			IF @@ROWCOUNT = 0
			BEGIN  
				SET @w_num_error = 50007 --30   Error ingresando secuencial de reverso

				GOTO ATM_ERROR
			END	
		END
	END

	--Valida Montos por tipo de pago
	IF @w_op_code LIKE '%560000%' -- P2P
	BEGIN
		-- Verificar monto minimo y maximo del pago p2p
		SELECT @w_monto_max = pa_money
		FROM  cobis..cl_parametro
		WHERE pa_producto = 'BV'
		AND   pa_nemonico = 'MAXPMR'

		SELECT @w_monto_min = pa_money
		FROM  cobis..cl_parametro
		WHERE pa_producto = 'BV'
		AND   pa_nemonico = 'MINPMR'	
	END

	IF @w_op_code LIKE '%560050%' -- P2C
	BEGIN
		SELECT @w_monto_max = pa_money
		FROM  cobis..cl_parametro
		WHERE pa_producto = 'BV'
		AND   pa_nemonico = 'MAXP2C'

		SELECT @w_monto_min = pa_money
		FROM  cobis..cl_parametro
		WHERE pa_producto = 'BV'
		AND   pa_nemonico = 'MINP2C'
	END

	IF @i_op_code LIKE '%560009%' OR @i_op_code LIKE '%200030%' -- C2P
	BEGIN	
		-- Verificar monto minimo y maximo del pago c2p
		SELECT @w_monto_max = pa_money
		FROM  cobis..cl_parametro
		WHERE pa_producto = 'BV'
		AND   pa_nemonico = 'MAXC2P'

		SELECT @w_monto_min = pa_money
		FROM  cobis..cl_parametro
		WHERE pa_producto = 'BV'
		AND   pa_nemonico = 'MINC2P'
	END

	IF @i_monto < @w_monto_min --and @i_ts='N'
	BEGIN
		SET @o_mensaje = 'Monto debe ser mayor o igual a Bs.' + convert(VARCHAR, @w_monto_min)

		SET @w_num_error = 160033 --13
		GOTO ATM_ERROR
	END

	IF @i_monto > @w_monto_max --AND @i_ts = 'N'
	BEGIN
		SET @o_mensaje = 'Monto debe ser menor o igual a Bs.' + convert(VARCHAR, @w_monto_max)

		SET @w_num_error = 160033 --13

		GOTO ATM_ERROR
		
	END	
	--Valida Montos por tipo de pago
	
	IF @w_socio = 'N'
	BEGIN          --************************************* No es Socio
		--Validar cliente destinatario segun el tipo de transaccion
		IF @w_op_code LIKE '%560000%' -- P2P
		BEGIN	
			--Valida el cliente destino
			IF isnull(@w_ente_dest,'') = ''
			BEGIN
				SELECT @w_ente_dest = en_ente
					,@w_ente_mis_dest = en_ente_mis
					,@w_nom_cliente_benef = en_nombre 
				FROM cob_bvirtual..bv_ente a
				WHERE en_ced_ruc = @i_cedula_dest
					AND en_tipo = 'P'
			
				IF @@ROWCOUNT = 0
				BEGIN
					-- cedula no existe en cobis 
					SET @w_num_error = 160040 --80
				
					GOTO ATM_ERROR	
				END
					
				SET @w_ente_dest = NULL

				SELECT @w_ente_dest = en_ente
					,@w_ente_mis_dest = en_ente_mis
					,@w_nom_cliente_benef = en_nombre
				FROM cob_bvirtual..bv_p2p
					,cob_bvirtual..bv_ente a
				WHERE pp_ente = en_ente
					AND en_ced_ruc = @i_cedula_dest
					AND en_tipo = 'P'
		
				IF @w_ente_dest IS NULL
				BEGIN
					-- cliente no afiliado a servicio de pagos
					SET @w_num_error = 160034 -- 14
				
					GOTO ATM_ERROR					
				END
				
				SET @w_ente_dest = NULL
					
				SELECT @w_ente_dest = en_ente
					,@w_ente_mis_dest = en_ente_mis
					,@w_nom_cliente_benef = en_nombre
				FROM cob_bvirtual..bv_medio_envio
					,cob_bvirtual..bv_ente a
				WHERE me_num_dir = @w_telefono_dest
					AND en_ced_ruc = @i_cedula_dest
					AND me_ente = en_ente
					AND en_tipo = 'P'

				IF @@rowcount = 0
				BEGIN
					-- el telefono no se encuentra en cobis 
					SET @w_num_error = 160038 --56

					GOTO ATM_ERROR					
				END
			END
		END
		
		IF @w_op_code LIKE '%560050%' -- P2C
		BEGIN
			-- buscar el ente destino segun el telefono destino
			IF isnull(@w_ente_dest,'') = ''
			BEGIN
				-- buscar el ente a quien hay que acreditarle
				SELECT @w_ente_dest = en_ente
					,@w_ente_mis_dest = en_ente_mis
					,@w_nom_cliente_benef = en_nombre
				FROM cob_bvirtual..bv_ente a
				WHERE en_ced_ruc = @i_cedula_dest
					AND en_tipo = 'C'

				IF @@rowcount = 0
				BEGIN
					-- cedula no existe en cobis 
					SET @w_num_error = 160038 --56
					
					GOTO ATM_ERROR					
				END

				SET @w_ente_dest = NULL

				SELECT @w_ente_dest = en_ente
					,@w_ente_mis_dest = en_ente_mis
					,@w_nom_cliente_benef = en_nombre
				FROM cob_bvirtual..bv_p2p
					,cob_bvirtual..bv_ente a
				WHERE pp_ente = en_ente
					AND en_ced_ruc = @i_cedula_dest

				IF @@rowcount = 0
				BEGIN
					-- cliente no afiliado 
					SET @w_num_error = 160034 --14
					
					GOTO ATM_ERROR					
				END
				
				SET @w_ente_dest = NULL

				SELECT @w_ente_dest = b.en_ente
					,@w_ente_mis_dest = b.en_ente_mis
					,@w_nom_cliente_benef = b.en_nombre
					,@w_ente_adm = a.en_ente
				FROM cob_bvirtual..bv_medio_envio
					,cob_bvirtual..bv_ente a
					,cob_bvirtual..bv_user_adm
					,cob_bvirtual..bv_ente b
				WHERE me_num_dir = @w_telefono_dest
					AND me_ente = a.en_ente
					AND a.en_ente_mis = ua_ente_adm
					AND ua_ente_jur = b.en_ente_mis
					AND b.en_tipo = 'C'
					AND b.en_ced_ruc = @i_cedula_dest

				IF @@rowcount = 0
				BEGIN
					-- el telefono no se encuentra en cobis
					SET @w_num_error = 160038 --56
					
					GOTO ATM_ERROR	
				END
			END
		END	
			
		IF @i_op_code LIKE '%560009%' OR @i_op_code LIKE '%200030%' -- C2P
		BEGIN
			SET @w_ente_dest = NULL

			-- buscar el ente a quien hay que debitarle
			SELECT @w_ente_dest = en_ente
				,@w_ente_mis_dest = en_ente_mis
				,@w_nom_cliente_benef = en_nombre
			FROM cob_bvirtual..bv_ente a
			WHERE en_ced_ruc = @i_cedula_dest
				AND en_tipo = 'P'

			IF @@rowcount = 0
			BEGIN
				-- cedula no existe en cobis --
				SET @w_num_error = 160040 --80

				GOTO ATM_ERROR		
			END			

			SET @w_ente_dest = NULL

			SELECT @w_ente_dest = en_ente
				,@w_ente_mis_dest = en_ente_mis
				,@w_nom_cliente_benef = en_nombre
			FROM cob_bvirtual..bv_p2p
				,cob_bvirtual..bv_ente a
			WHERE pp_ente = en_ente
				AND en_ced_ruc = @i_cedula_dest

			IF @@rowcount = 0
			BEGIN
				-- cliente no afiliado 
				SET @w_num_error = 160034 --14

				GOTO ATM_ERROR						
			END

			SET @w_ente_dest = NULL

			-- verificar que exista la dupla cedula + telefono
			SELECT @w_ente_dest = en_ente
				,@w_ente_mis_dest = en_ente_mis
				,@w_nom_cliente_benef = en_nombre
				,@w_celular2 = '58' + right(me_num_dir, 10)
			FROM cob_bvirtual..bv_medio_envio
				,cob_bvirtual..bv_ente a
			WHERE  me_ente = en_ente
				AND me_servicio = 1
				AND me_num_dir = @w_telefono_dest
				AND en_ced_ruc = @i_cedula_dest

			IF @@rowcount = 0
			BEGIN
				-- cliente no afiliado a servicio de pagos 
				SET @w_num_error = 160038 -- 56

				GOTO ATM_ERROR					
			END
/*
			-- buscar el ente cobrador segun el telefono origen
			SELECT @w_ente_adm = a.en_ente
					,@w_celular2 = '58' + right(me_num_dir, 10)
					,@w_ente_mis_dest = ua_ente_jur
			FROM cob_bvirtual..bv_medio_envio
				,cob_bvirtual..bv_ente a
				,cob_bvirtual..bv_user_adm
				,cob_bvirtual..bv_ente b
			WHERE me_num_dir = @w_telefono_org
			AND   me_ente = a.en_ente
			AND   a.en_ente_mis = ua_ente_adm
			AND   ua_ente_jur = b.en_ente_mis
			AND   b.en_tipo = 'C'

			IF @w_ente_adm IS NULL 
			BEGIN
				-- cliente no afiliado a servicio de pagos 
				SET @w_num_error = 160038 -- 56

				GOTO ATM_ERROR	   
			END*/
/*
			--validar token 
			IF ISNULL(@w_ente_dest,0) > 0 AND @i_otp != '-1'
			BEGIN
				SET @w_otp = right('00000000' + @i_otp, 8)

				EXEC @w_return = cob_bvirtual..sp_genera_token_cw_bv
					@s_cliente    = @w_ente_dest,
					@s_servicio   = 3,
					@s_date       = @w_fecha_proc,
					@i_operacion  = 'VT',
					@i_token_recibido = @i_otp
				
				IF @w_return <> 0 --Si retorna 0 la OTP es valida
				BEGIN
					SET @w_num_error = 169026 --55

					GOTO ATM_ERROR 
				END
			END
*/
			-- validar cupo C2P
			IF @w_corr <> 'R'
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
					--SET @o_mensaje = @o_mensaje --'CUPO INSUFICIENTE'
					SET @w_num_error = 160033 --13

					GOTO ATM_ERROR 
				END
			END			
		END
		
		--- busco el alias de la cuenta principal del destino
		BEGIN TRY
			EXEC @w_return = cob_bvirtual..tic_sp_cons_cuentas_bv 
					@s_cliente = @w_ente_dest 				--codigo del cliente en BV
				,@s_servicio = @w_canal						--codigo del canal
				,@i_operacion = 'C'							--codigo indicador de la operacion
				,@o_producto = @w_producto_dest OUTPUT		--retorna el codigo del producto cobis
				,@o_cuenta = @w_cta_banco_dest OUTPUT 		--retorna el valor numero de cuenta-producto

			IF @w_return <> 0
			BEGIN
				SET @w_num_error = 169032---71
					
				GOTO ATM_ERROR
			END
		END TRY
		BEGIN CATCH
			SET @w_num_error = 169032---71
				
			GOTO ATM_ERROR		
		END CATCH	

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
				SET @w_num_error = 169032---71
					
				GOTO ATM_ERROR					
			END
		END		
		
		SET @o_cta_dest = @w_cta_banco_dest
	END -- Fin Validar ente	

	SET @o_retorno = '00'
	SET @o_cta_dest = @w_cta_banco_dest
	SET @o_mensaje = 'TRANSACCION EXITOSA'
	SET @w_num_error = 0

	GOTO SALIR

	ATM_ERROR:
		BEGIN
			SELECT @o_mensaje = mensaje,
				   @o_retorno = er_error_red 
			FROM cob_atm..tm_errores_red
				,cobis..cl_errores
			WHERE er_error_cobis = numero
				AND er_error_cobis = @w_num_error
				AND er_srv = 'P2P'

			IF isnull(@o_mensaje,'') = ''
				SET @o_mensaje = 'TRANSACCION FALLIDA'

			IF isnull(@o_retorno,'') = '' --OR isnull(@o_retorno,'') != '00'
				SET @o_retorno = '12' 	
		END

	SALIR:
		BEGIN TRY
			IF @o_retorno = '00'
				SET @w_retorno = NULL
			ELSE
				SET @w_retorno = @o_retorno
				
			SELECT @w_fecha_log = MAX(pl_hora)
			FROM   cob_atm..tm_p2p_log
			WHERE pl_trace = @i_trace
			AND   pl_telefono_d = @i_telefono_dest
			AND   pl_referencia = @i_bit37
			AND   pl_corr = @w_corr				

			--Actualizar resultado del proceso en el log de Banca Virtual
			UPDATE tm_p2p_log
			SET pl_hora2 = getdate()
				,pl_result = @w_retorno
				,pl_error = @w_num_error
				,pl_procesar = NULL
			WHERE pl_trace = @i_trace
			AND   pl_telefono_o = @i_telefono_org
			AND   pl_telefono_d = @i_telefono_dest
			AND   pl_referencia = @i_bit37
			AND   pl_corr = @i_corr
			AND   pl_hora = @w_fecha_log
		END	TRY
		BEGIN CATCH
			SET @o_retorno = '12'
		END CATCH			
END
RETURN 0;