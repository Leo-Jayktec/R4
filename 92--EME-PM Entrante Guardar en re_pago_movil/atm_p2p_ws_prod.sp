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
/************************************************************/

CREATE PROCEDURE [dbo].[atm_p2p_ws] (
	@i_srv_org			VARCHAR(30) = ''
	,@i_corr			CHAR(1)     = 'N'					--N=No, S=Si-Reverso
	,@i_telefono_org	VARCHAR(12) = NULL
	,@i_cedula_orig		VARCHAR(20) = NULL	
	,@i_banco_org		VARCHAR(16) = NULL
	,@i_telefono_dest	VARCHAR(12) = NULL
	,@i_cedula_dest		VARCHAR(20) = NULL
	,@i_op_code			VARCHAR(8) = NULL
	,@i_bit37		    VARCHAR(12) = NULL				    --Referencia del Emisor (bit37)
	,@i_trace			VARCHAR(64) = NULL
	,@i_monto			MONEY = NULL
	,@i_otp             NUMERIC(8) = NULL
	,@o_retorno			VARCHAR(2) = NULL OUTPUT
	,@o_mensaje			VARCHAR(200) = NULL OUTPUT
	,@o_ssn_branch      INT        = NULL OUTPUT
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
			,@w_cta_orig        VARCHAR(10)
			,@w_signo           CHAR(1)
			,@w_tran_servicio   CHAR(1)
			,@w_est_notifica    VARCHAR(4)
			,@w_nombre_dest     VARCHAR(64)
			,@w_fecha_iso       VARCHAR(50)
			,@w_correo_cobra    VARCHAR(255)
			,@w_fecha_notif     VARCHAR(50)
			,@w_tipo            VARCHAR(10)
			,@w_manejo_cola     CHAR(1)
			,@w_cedula_dest     VARCHAR(20)
			,@w_fecha_log		DATETIME
			,@w_log_corr        CHAR(1)
			,@w_clave_trace		VARCHAR(64)
			,@w_telefono_soc	VARCHAR(12)
			
	IF @i_corr = 'S'
		SET @w_corr = 'R'
	ELSE
		SET @w_corr = @i_corr

	SET @w_log_corr = @w_corr
	SET @w_ing_tran = 'N'
	SET @w_socio = 'N'
	SEt @w_telefono_soc = @i_telefono_dest
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
	SEt @w_signo = 'C'
	SEt @w_tipo = 'RECIBIDA'
	SET @w_manejo_cola  = 'N'
	SET @w_tran_servicio = 'N'
	SET @w_telefono_org = '0' + right(@i_telefono_org,10)
	SET @w_telefono_dest = '0' + right(@i_telefono_dest,10)
	
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
							WHEN @w_op_code = '560000'
								THEN 'P2P'
							WHEN @w_op_code = '560009'
								THEN 'C2P'
							WHEN @w_op_code = '560050'
								THEN 'P2C'
							WHEN @w_op_code = '200030'    --JT-30102024
								THEN 'C2P'
						END

	--Consultar si el destino es cliente del banco con la identificacion envida
	SELECT @w_cedula_dest = en_ced_ruc
	FROM   cob_bvirtual..bv_ente
	WHERE  en_ced_ruc = @i_cedula_dest

	--Si no existe se eliminan los posibles 0 a la izquierda
	IF @w_cedula_dest IS NULL
		select @i_cedula_dest = convert(VARCHAR, convert(NUMERIC, cob_atm.dbo.numero(@i_cedula_dest)))

	--Buscar Secuencial de reverso
	IF @w_op_code = '200030' AND @w_corr = 'N'   --Anulacion
	BEGIN
		SELECT @w_sec_reverso = sr_sec_reverso
			  ,@w_clave_trace = sr_clave_trace
		FROM   cob_atm..tm_secuencial_reverso
		WHERE  sr_bit37 = @i_telefono_org
        AND    substring(sr_clave_trn, 1, 1) != 'R'
	END
	ELSE
	BEGIN
		IF @w_op_code = '200030' AND @w_corr = 'R'
		BEGIN
			SELECT @w_sec_reverso = sr_sec_reverso
			FROM  cob_atm..tm_secuencial_reverso
			WHERE sr_clave_trn = @i_trace
			and   sr_bit37 = @i_bit37
		END
		ELSE
		BEGIN
			SELECT @w_sec_reverso = sr_sec_reverso
			FROM   cob_atm..tm_secuencial_reverso
			WHERE  sr_clave_trn = @i_trace	
		END
	END

	--En caso de reverso buscar los numeros de la transaccion original - Cambio por reverso de Vuelto
	IF @w_corr = 'R' 
	BEGIN
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
		
	--Validar si el destino es un socio
	SELECT @w_ente_orig = en_ente,
	       @w_ente_mis_dest = en_ente_mis,
		   @w_cta_banco_dest = ps_cta_banco,
		   @w_producto_dest = ps_producto,
		   @w_tran_servicio = ps_tran_servicio,
		   @w_est_notifica = ps_estado,
		   @w_nom_cliente_benef = ps_nombre
	FROM   cob_bvirtual..bv_ente
		INNER JOIN cob_atm..tm_p2p_socio
		ON en_ente_mis = ps_ente_mis
	WHERE  ps_telefono = '0' + right(@w_telefono_soc,10)

	IF @@ROWCOUNT > 0
	BEGIN
		SET @w_socio = 'S'

		SELECT @w_correo_cobra = me_num_dir
		FROM   cob_bvirtual..bv_medio_envio
		WHERE  me_tipo = 'MAIL'
		AND    me_default = 'S'
		AND    me_ente = @w_ente_orig
	END

	IF (@w_corr = 'R' OR @w_op_code = '200030')--En caso de reverso guardar el registro del log
	BEGIN
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
				,@w_op_code
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
	END

	--Si es socio no es factible realizar un C2P entrante
	IF @w_socio = 'S' AND @w_tipo_pago = 'C2P'
	BEGIN
		SET @w_num_error = 160034 --TRANSACCION NO PERMITIDA

		GOTO ATM_ERROR    	
	END		
		
	--Validar banco emisor
	IF ISNUMERIC(@i_banco_org) = 1
	BEGIN
		SET @w_banco_pago = convert(INT, @i_banco_org)
		SET @w_banco_char = convert(CHAR(3), @w_banco_pago)
		
		SELECT @w_habilitado = c.codigo
		FROM   cobis..cl_tabla t
			INNER JOIN cobis..cl_catalogo c
			ON t.codigo = c.tabla
		WHERE t.tabla = 'cl_bancos_ptp'
		AND   c.estado = 'V'
		AND   c.codigo = '0' + @w_banco_char
		
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
	
	IF @w_op_code = '200030' AND @w_corr = 'N'   --Anulacion
	BEGIN
		IF isnull(@w_sec_reverso,0) = 0
		BEGIN
			SET @w_num_error = 160035 --30 160036

			GOTO ATM_ERROR    
		END

		-- Valido C2P original
		SELECT @i_telefono_org = pl_telefono_o
		FROM   cob_atm..tm_p2p_log
		where  pl_trace = @w_clave_trace
		and    pl_op_code = '560009'
		and    pl_result = '00'
		and    pl_telefono_d = @i_telefono_dest
		and    pl_monto = @i_monto
		
		IF @@ROWCOUNT = 0
		BEGIN
			SET @w_num_error = 160035 --30 160036

			GOTO ATM_ERROR    
		END
	END
	ELSE
	BEGIN
		IF @w_op_code = '200030' AND @w_corr = 'R'
		BEGIN
			IF isnull(@w_sec_reverso,0) = 0
			BEGIN
				SET @w_num_error = 160035 --30 

				GOTO ATM_ERROR
			END
		END
		ELSE
		BEGIN		
			IF isnull(@w_sec_reverso,0) <> 0 AND @w_corr = 'N'
			BEGIN
				SET @w_num_error = 160035--30
					
				GOTO ATM_ERROR
			END			
		END
	END

	IF isnull(@w_sec_reverso,0) = 0 AND @w_corr = 'R'
	BEGIN
		SET @w_num_error = 160035 --30 

		GOTO ATM_ERROR
	END
	
	SELECT @w_result = pl_result
	FROM   cob_atm..tm_p2p_log
	WHERE  pl_trace = @i_trace
	AND    pl_referencia = @i_bit37
	AND    pl_corr = 'N'	
		
	IF @@ROWCOUNT = 0
	BEGIN
		SET @w_num_error = 50004 --NO EXISTE LA TRASACCION DE VERIFICACION

		GOTO ATM_ERROR
	END

	IF ISNULL(@w_result,'') = '' AND @w_corr = 'R'
	BEGIN
		SET @w_num_error = 50004 --NO EXISTE LA TRASACCION DE VERIFICACION

		GOTO ATM_ERROR
	END

	--Busca el ssn_branch que se refleja en la tran_monet
	EXEC cob_atm..sp_cseqnos 
	 	 @i_tabla = 'tm_compra_pos'
		,@o_siguiente = @w_ssn_branch OUT

	SET @o_ssn_branch = @w_ssn_branch

	--En caso de reverso busco el monto original de la transaccion
	IF @w_corr = 'R'
	BEGIN
		IF @w_result <> '00'
		BEGIN			
			SET @w_num_error = 160035 --30   Error ingresando secuencial de reverso

			GOTO ATM_ERROR
		END
	END
	
	--Valida Montos por tipo de pago
	IF @w_op_code = '560000' -- P2P
	begin
		-- Verificar monto minimo y maximo del pago p2p
		SELECT @w_monto_max = pa_money
		FROM  cobis..cl_parametro
		WHERE pa_producto = 'BV'
		AND   pa_nemonico = 'MAXPMR'

		SELECT @w_monto_min = pa_money
		FROM  cobis..cl_parametro
		WHERE pa_producto = 'BV'
		AND   pa_nemonico = 'MINPMR'	
	end

	IF @w_op_code = '560050' -- P2C
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

	IF @w_op_code = '560009' OR @w_op_code = '200030' -- C2P
	BEGIN	
		SET @w_signo = 'D' --Debito
		SEt @w_tipo = 'ENVIADA'
		
		IF @w_op_code = '200030' and @i_corr = 'N' --Anulacion --JT-30102024
		begin
			SET @w_signo = 'C' --Credito
			SEt @w_tipo = 'RECIBIDA'		
		END

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

	--Validar si el Ente receptor es socio
	if @w_socio = 'N' --************************************* No Es Socio
	BEGIN          	
		--Validar cliente destinatario segun el tipo de transaccion
		IF @w_op_code = '560000' -- P2P
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
				WHERE me_num_dir = '0' + right(@i_telefono_dest,10)
					AND en_ced_ruc = @i_cedula_dest
					AND me_ente = en_ente
					AND en_tipo = 'P'

				IF @@rowcount = 0
				BEGIN
					-- el telefono no se encuentra en cobis 
					SET @w_num_error = 160038 --56

					GOTO ATM_ERROR					
				END

				SELECT @w_correo_cobra = me_num_dir
				FROM   cob_bvirtual..bv_medio_envio
				WHERE  me_tipo = 'MAIL'
				AND    me_default = 'S'
				AND    me_ente = @w_ente_dest
			END
		END
		
		IF @w_op_code = '560050' -- P2C
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
				WHERE me_num_dir = '0' + right(@i_telefono_dest,10)
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

				SELECT @w_correo_cobra = me_num_dir
				FROM   cob_bvirtual..bv_medio_envio
				WHERE  me_tipo = 'MAIL'
				AND    me_default = 'S'
				AND    me_ente = @w_ente_adm
			END
		END	
			
		IF @w_op_code = '560009' OR @w_op_code = '200030' -- C2P
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
			WHERE en_ced_ruc = @i_cedula_dest
				AND me_ente = en_ente
				AND me_servicio = 1
				AND me_num_dir = '0' + right(@i_telefono_dest, 10)

			IF @@rowcount = 0
			BEGIN
				-- cliente no afiliado a servicio de pagos 
				SET @w_num_error = 160038 -- 56

				GOTO ATM_ERROR					
			END

			SELECT @w_correo_cobra = me_num_dir
			FROM   cob_bvirtual..bv_medio_envio
			WHERE  me_tipo = 'MAIL'
			AND    me_default = 'S'
			AND    me_ente = @w_ente_dest

			--validar token 
			IF @w_op_code = '560009' --ISNULL(@w_ente_dest,0) > 0 AND @i_otp != '-1' AND 
			BEGIN
				SET @w_otp = right('00000000' + @i_otp, 8) 
  
				EXEC @w_return = cob_bvirtual..sp_genera_token_cw_bv
					@s_cliente    = @w_ente_dest,
					@s_servicio   = 3,
					@s_date       = @w_fecha_proc,
					@i_from       = 'P',					
					@i_operacion  = 'P',
					@i_login      = NULL,
					@i_token_recibido = @w_otp
			
				IF @w_return <> 0 --Si retorna 0 la OTP es valida
				BEGIN
					SET @w_num_error = 169026 --55

					GOTO ATM_ERROR 
				END
			END

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

	IF ISNULL(@w_sec_reverso,0) = 0 AND @i_corr = 'N'
		SEt @w_sec_reverso = 0

	--Hora del registro en la tran_monet de remesas
	SET @w_hora = getdate()
	SET @w_concepto = 'DE:' + substring(@w_telefono_org, 3, 9) + ' REF:' + substring(@w_telefono_dest, 3, 9)
	SET @w_cta_orig = right('0000' + isnull(@i_banco_org, ''), 4) + isnull(@w_telefono_org, '')
	
	IF @w_corr = 'N' --Solo se ingresa la transaccion cuando no es reverso
	BEGIN
		--EJECUTAR TRANSACCION DE servicio
		BEGIN TRY
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
					--,@i_ssn_host = @w_sec ---------
					,@i_hora = @w_hora
					,@i_srv_host = @w_srv_host ---------
					,@i_estado_ejec = 'EV'
					,@i_tipo_ejec = 'L'
					,@i_ofi_cta = @w_oficina_org
					,@i_tel_ori = @w_telefono_org
					,@i_tel_des = @w_telefono_dest
					,@i_ced_ruc = @i_cedula_dest

			IF @w_return <> 0
			BEGIN
				SET @w_num_error = @w_return
				GOTO ATM_ERROR
			END	

			--Indicador que registro la transaccion en tran_monet remesas local
			SET @w_ing_tran = 'S'
		END TRY
		BEGIN CATCH
			IF ISNULL(@w_return, '') = ''
				SET @w_return = 40007

			SET @w_num_error = @w_return

			GOTO ATM_ERROR
		END CATCH
	END

	--EJECUTAR TRANSACCION MONETARIA
	BEGIN TRY
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
				,'cobisdatos'						--s_user
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

			) at SYBPRODLINK

		if isnull( @w_return,0) != 0
		BEGIN
			SET @w_num_error = @w_return

			GOTO ATM_ERROR
		END
	END TRY
	BEGIN CATCH
		IF ISNULL(@w_return, '') = ''
			SET @w_return = 40006
			
		SET @w_num_error = @w_return
		GOTO ATM_ERROR
	END CATCH
	
	IF @w_corr = 'N' --Solo se actualiza la transaccion cuando no es reverso
	BEGIN
		BEGIN TRY

			INSERT INTO cob_atm..tm_secuencial_reverso
			VALUES ( @i_trace
					,@i_trace
					,@w_sec
					,@w_oficina_org
					,@i_bit37 )
				
			IF @@error != 0
			BEGIN  
				SET @w_num_error = 160035 --30   Error ingresando secuencial de reverso

				GOTO ATM_ERROR
			END	
			
			UPDATE cob_remesas..re_tran_monet
			SET tm_estado_ejecucion = 'EJ'
				,tm_ssn_host = @w_sec
				,tm_saldo_contable = @w_saldo_conta
				,tm_saldo_disponible = @w_saldo_dispo
				,tm_oficina_cta = @w_ofi_cta_ori
			WHERE tm_ssn_local = @w_ssn_branch
			  AND tm_filial = 1
			  AND tm_cta_banco = @w_cta_banco_dest

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
					
/*
				if ISNULL(@w_return,0) <> 0 
				BEGIN
					SET @w_num_error = @w_return

					GOTO ATM_ERROR
				END*/
			END 

			EXEC cob_bvirtual..sp_p2p_est 
				 @i_banco_org
				,'I'
				,'S'
				,@i_monto
		END TRY
		BEGIN CATCH
			UPDATE cob_remesas..re_tran_monet
			SET tm_estado_ejecucion = 'EE'
				,tm_ssn_host = @w_sec
			WHERE tm_ssn_local = @w_ssn_branch
				AND tm_filial = 1
	/*
			SET @w_num_error = @w_return
			GOTO ATM_ERROR*/
		END CATCH
	
		-- ACTUALIZAR CUPO DE LA TRANSACCION MONETARIA EN LA TDD ---
		IF @w_op_code LIKE '%560009%' --OR @i_op_code LIKE '%200030%'   --JT-30102024
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
/*
			IF @w_return <> 0
			BEGIN
				SET @w_num_error = @w_return
				GOTO ATM_ERROR
			END*/
		END

		--Notificacion al destinatario si no es socio o los socios pero sin utilizar la cola de notas de credito
		IF @w_socio = 'N' OR @w_manejo_cola = 'N'
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
/*
			IF @w_return <> 0
			BEGIN
				SET @w_num_error = @w_return
				GOTO ATM_ERROR
			END*/

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
	/*
				IF @w_return <> 0
				BEGIN
					SET @w_num_error = @w_return
					GOTO ATM_ERROR
				END*/

				EXEC @w_return = msdb..sp_send_dbmail 
					 @recipients = @w_correo_cobra
					,@body = @w_mensaje
					,@body_format = 'HTML'
					,@subject = 'R4 Banco Movil'
					,@exclude_query_output = 1
/*
				IF @w_return <> 0
				BEGIN
					SET @w_num_error = @w_return
					GOTO ATM_ERROR
				END*/
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


	SET @o_retorno = '00'
	SET @o_cta_dest = @w_cta_banco_dest
	SET @o_mensaje = 'TRANSACCION EXITOSA'
	SET @w_num_error = 0

GOTO SALIR

ATM_ERROR:
	BEGIN
		BEGIN TRY
			IF @w_corr != 'R'
			BEGIN
				EXEC cob_bvirtual..sp_p2p_est 
						@i_banco = @i_banco_org
					,@i_dir   = 'I'
					,@i_exito = 'N'
					,@i_monto = @i_monto	

				IF @w_ing_tran = 'S' and @w_ssn_branch > 0
					UPDATE cob_remesas..re_tran_monet
					SET    tm_estado_ejecucion = 'EE'
							,tm_control = @w_num_error
					WHERE tm_ssn_local = @w_ssn_branch	
						AND tm_filial = 1
		                AND tm_cta_banco = @w_cta_banco_dest
			END		
		END TRY
		BEGIN CATCH
			SET @o_mensaje = 'TRANSACCION FALLIDA'
			SET @o_retorno = '12' 		
		END CATCH			
	
		SELECT @o_mensaje = mensaje,
			   @o_retorno = er_error_red 
		FROM cob_atm..tm_errores_red
			,cobis..cl_errores
		WHERE er_error_cobis = numero
			AND er_error_cobis = @w_num_error
			AND er_srv = 'P2P'

		IF isnull(@o_mensaje,'') = ''
			SET @o_mensaje = 'TRANSACCION FALLIDA'

		IF isnull(@o_retorno,'') = '' 
			SET @o_retorno = '12' 	
	END

SALIR:
	BEGIN TRY	
		-- JT - 01/24/2025 Validacion de consistencia de error cobis y error de red
		IF ISNULL(@w_num_error,0) != 0 OR ISNULL(@w_retorno,'') = '00'
		BEGIN
			SET @o_retorno = '12'
			SET @o_mensaje = 'TRANSACCION FALLIDA'
		END

		SELECT @w_fecha_log = MAX(pl_hora)
		FROM   cob_atm..tm_p2p_log
		WHERE pl_trace = @i_trace
		AND   pl_telefono_d = @i_telefono_dest
		AND   pl_referencia = @i_bit37
		AND   pl_corr = @w_log_corr--@w_corr
		
		--Actualizar resultado del proceso en el log de Banca Virtual
		UPDATE tm_p2p_log
		SET pl_hora2 = getdate()
			,pl_result = @o_retorno
			,pl_error = @w_num_error
			,pl_procesar = NULL
		WHERE pl_trace = @i_trace
		AND   pl_telefono_d = @i_telefono_dest
		AND   pl_referencia = @i_bit37
		AND   pl_corr = @w_log_corr--@w_corr
		AND   pl_hora = @w_fecha_log
		
		--Actualizar informacion para reporte de pago telefonico
		SET @w_cta_banco_dest = ISNULL(@w_cta_banco_dest,'')
		SET @w_ssn_branch = ISNULL(@w_ssn_branch,0)
		SET @w_sec = ISNULL(@w_sec,0)
	
		EXEC @w_num_error = cob_bvirtual..sp_log_p2p 
			 @i_servicio = @w_tipo_pago
			,@i_telefono_o = @i_telefono_org
			,@i_telefono_d = @i_telefono_dest
			,@i_monto = @w_monto
			,@i_banco = @w_banco_char
			,@i_cta_banco = @w_cta_banco_dest
			,@i_reverso = @w_corr
			,@i_codigo_respuesta = @o_retorno
			,@i_error_cobis = @w_num_error
			,@i_msg = @o_mensaje
			,@i_referencia = @i_bit37
			,@i_tipo = @w_tipo
			,@i_ssn_branch = @w_ssn_branch
			,@i_manejo_cola = @w_manejo_cola
			,@i_signo = @w_signo
	        ,@i_secuencial = @w_sec
	
		IF ISNULL(@w_num_error ,0) > 0
		BEGIN
			insert into cob_atm..tm_reconstruir_log 
			values (@w_tipo_pago, @i_telefono_org, @i_telefono_dest, @w_monto, @w_banco_char, @w_cta_banco_dest, 
                    @w_corr, @o_retorno, @w_num_error, @i_bit37, @w_tipo, @w_ssn_branch,@w_signo,@w_sec, @o_mensaje, getdate())
		END
	END TRY
	BEGIN CATCH
			insert into cob_atm..tm_reconstruir_log 
			values (@w_tipo_pago, @i_telefono_org, @i_telefono_dest, @w_monto, @w_banco_char, @w_cta_banco_dest, 
                    @w_corr, @o_retorno, @w_num_error, @i_bit37, @w_tipo, @w_ssn_branch,@w_signo,@w_sec,@o_mensaje, getdate())
	END CATCH
END

RETURN 0
