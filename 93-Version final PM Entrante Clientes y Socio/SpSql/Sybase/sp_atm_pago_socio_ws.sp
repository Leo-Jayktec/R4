use cob_cuentas
GO

IF OBJECT_ID('dbo.sp_atm_pago_socio_ws') IS NOT NULL
BEGIN
	DROP PROCEDURE dbo.sp_atm_pago_socio_ws 

	IF OBJECT_ID('dbo.sp_atm_pago_socio_ws') IS NOT NULL
		PRINT '<<< FAILED DROPPING PROCEDURE dbo.sp_atm_pago_socio_ws  >>>'
	ELSE
		PRINT '<<< DROPPED PROCEDURE dbo.sp_atm_pago_socio_ws  >>>'
END
go

/************************************************************/
/* NOMBRE ARCHIVO: sp_atm_pago_socio_ws.sp                  */
/* BASE DE DATOS: cob_cuentas (Sybase)                      */
/* PROCESA TRANSACCION MONETARIA PAGO MOVIL ENTRANTE SOCIOS */
/* 12/09/2024  Emision inicial   Jayktec                    */
/************************************************************/

create proc sp_atm_pago_socio_ws (
	@s_user               varchar(30),
	@t_corr               char(1)       = 'N',
	@t_ssn_corr           int           = null,
	@i_cliente			  int,
	@i_producto           smallint,
	@i_cta_banco          cuenta,
	@i_val                money,	
	@i_banco              char(4),
	@i_referencia         int,
	@i_concepto           descripcion,
	@i_tran_servicio      char(1) = 'N',
	@i_trace			  varchar(64),
	@i_op_code            varchar(8),
	@i_cta_orig           varchar(20),
	@i_tipo_ced_org	      char(1),
	@i_cedula_org	      numero,
	@i_telefono_org	      varchar(12),
	@i_tipo_ced_dest      char(1),
	@i_cedula_dest	      numero,
	@i_telefono_dest	  varchar(12),	
	@i_nom_cliente_benef  varchar(64),
	@o_sec                int           = null out,
    @o_resultado		  VARCHAR(3)    = NULL OUT,
	@o_cod_error		  INT           = NULL OUT,
	@o_mensaje			  VARCHAR(200)  = NULL OUT
)
as

declare
    @s_ssn                		int,
	@s_srv                      varchar(30)   ,
	@s_lsrv                     varchar(30)   ,
	@s_term                     varchar(10),
	@s_date                     datetime,
	@s_ofi                      int,          -- Localidad origen transaccion 
	@w_corr                     char(1),
	@w_return              		int,
	@w_cod_error                int,
	@w_sp_name                 	varchar(30),
	@w_prod_habilitado_des		char(1),
	@w_rpc                    	descripcion,
	@w_ssn                      int,
	@w_moneda                 	tinyint,
	@w_ofi_cta					int,
	@w_cuenta           	   	int,	
	@w_trn      				int,		
	@w_trn_com     				int,
	@w_causa              		varchar(3),
	@w_causa_com          		varchar(3),
	@w_nombre_benef             descripcion,
	@w_cuenta_int               int,
	@w_trn_p2p                  varchar(5),
	@w_telefono_socio			varchar(12),
	@w_signo                    char(1),
	@w_tipo                     VARCHAR(10),
	@w_cedula_dest              VARCHAR(20),
	@w_referencia               descripcion,
	@w_tipo_pago				varchar(30),
	@w_subtipo                  char(1),
	@w_habilitado               varchar(10),
	@W_tipo_telefono            CHAR(10),
	@w_porcentaje_comision 	    money,
	@w_monto_limite             money,
	@w_mto_comision             money,
	@w_mto_com_socio             money,
	@w_porc_com_socio			money,
	@w_banco                    int,
	@w_mto_com_min				money,
	@w_mto_com_neg				money,
	@w_concepto_com				descripcion,
	@w_concepto_com_neg			descripcion,
	@w_param_cau				VARCHAR(10),
	@w_param_cau_com			VARCHAR(10),
	@w_par_mto_min   			VARCHAR(10),
	@w_par_porc_com   			VARCHAR(10),
	@w_par_com_min   			VARCHAR(10),
	@w_cta_orig                 VARCHAR(20)

BEGIN
	--Inicializa Variables
	SET @w_sp_name  = 'sp_atm_pago_socio_ws'
	SET @o_sec   = 0
	SET @w_ofi_cta = 0
	SET @w_mto_comision = 0
	SET @w_cod_error = 0
	SET @w_referencia = RIGHT('000000000000' + CONVERT(VARCHAR(10),@i_referencia), 12)
	SET @w_banco = CONVERT(INT, @i_banco)

	SELECT @s_date = fp_fecha
	from   cobis..ba_fecha_proceso

	IF @i_tran_servicio = 'S'
		SET @w_trn_p2p = 'P2PSW'
	ELSE
		SET @w_trn_p2p = 'P2P'

	IF @i_op_code = '560000' 
		SET @w_tipo = 'P2P'
	
	if @i_op_code='560050'
		SET @w_tipo = 'P2C'

	SET @w_tipo_pago = 'RECIBIDA'
	SET @w_signo = 'C'
	--SET @w_subtipo = 'C'

	IF ISNULL(@t_corr, 'N') <> 'N'
		SET @w_corr = 'R'
	ELSE
		SET @w_corr = 'N'

	SELECT @w_cedula_dest = en_ced_ruc,
	       @w_subtipo     = en_subtipo 
	FROM cobis..cl_ente
	WHERE en_ente = @i_cliente
	AND   en_tipo_ced = ISNULL(@i_tipo_ced_dest,en_tipo_ced)
	AND   en_ced_ruc = @i_cedula_dest
	--AND   en_subtipo =  @w_subtipo

	-- Si no existe se eliminan los posibles 0 a la izquierda
	IF @w_cedula_dest IS NULL 
		SELECT @i_cedula_dest = CONVERT(VARCHAR, CONVERT(NUMERIC, @i_cedula_dest))

	SELECT @w_cedula_dest = en_ced_ruc,
	       @w_subtipo     = en_subtipo 
	FROM cobis..cl_ente
	WHERE en_ente = @i_cliente
	AND   en_tipo_ced = ISNULL(@i_tipo_ced_dest,en_tipo_ced)
	AND   en_ced_ruc = @i_cedula_dest
	--AND   en_subtipo =  @w_subtipo

	IF @w_cedula_dest IS NULL 
		SET @w_cod_error = 160040 -- cedula no existe en cobis	

	IF @w_subtipo = 'P' AND @i_op_code='560050'
		SET @w_cod_error = 160040 -- cedula no existe en cobis	

	--Validar banco emisor
	IF ISNUMERIC(@i_banco) = 1 
	BEGIN
		IF @w_cod_error = 0
		BEGIN
			SELECT @w_habilitado = c.codigo
			FROM   cobis..cl_tabla t
				INNER JOIN cobis..cl_catalogo c
				ON t.codigo = c.tabla
			WHERE t.tabla = 'cl_bancos_ptp'
			AND   c.estado = 'V'
			AND   c.codigo = @i_banco
			
			IF @@rowcount = 0
				SET @w_cod_error = 160042
		END
	END
	ELSE
		SET @w_cod_error = 160042

	--SET @w_telefono_socio = '0' + RIGHT(@i_telefono_dest,10)

	SELECT @w_porc_com_socio = cc_comision,
	       @w_telefono_socio = cc_telefono
	FROM   cob_remesas..re_p2p_rec
	WHERE  cc_ente     = @i_cliente
	--AND    cc_telefono = @w_telefono_socio
	AND    cc_cta_banco = @i_cta_banco

	IF @@ROWCOUNT > 0
	BEGIN
		IF @w_telefono_socio = '0' + RIGHT(@i_telefono_dest,10)
			SET @w_mto_com_socio = @i_val * (isnull(@w_porc_com_socio,0) /100)
		ELSE 
			SET @w_cod_error = 160038
	END
	ELSE
		SET @w_cod_error = 160034 

	IF @w_cod_error = 0  --1
	BEGIN
	    -- Obtiene el nombre del servidor remoto
		SELECT @s_srv = pa_char
		FROM   cobis..cl_parametro
		WHERE  pa_nemonico = 'SRVR'
		AND    pa_producto = 'ADM'

		--Oficina segun servidor
		SELECT @s_ofi = ro_oficina
		FROM   cob_atm..tm_redes_oficinas
		WHERE  ro_red = 'CONEXUS'

		--Validar cuenta destino
		if @i_producto = 3
		BEGIN
			select @w_rpc = 'cob_cuentas..sp_ccndc_automatica_bv'

			select @w_cuenta_int = cc_ctacte,
				@w_moneda    = cc_moneda,
				@w_nombre_benef = cc_nombre,
				@w_ofi_cta = cc_oficina
			from  cob_cuentas..cc_ctacte
			where cc_cta_banco = @i_cta_banco
			and   cc_cliente = @i_cliente
			and   cc_estado    = 'A'
			and   cc_moneda    = 0

			if @@rowcount != 1
				SET @w_cod_error = 201004
			
			IF @w_signo = 'C'
				SELECT @w_trn = 48
			ELSE
				SELECT @w_trn = 50

			SELECT @w_trn_com = 50
		END
		ELSE
		BEGIN
			select @w_rpc = 'cob_ahorros..sp_ahndc_automatica_bv'

			select @w_cuenta_int = ah_cuenta,
				@w_moneda    = ah_moneda,
				@w_nombre_benef = ah_nombre,
				@w_ofi_cta = ah_oficina 
			from cob_ahorros..ah_cuenta
			where ah_cta_banco = @i_cta_banco
			and   ah_cliente = @i_cliente
			and   ah_estado    = 'A'
			and   ah_moneda    = 0
			
			if @@rowcount != 1
				SET @w_cod_error = 201004	

			IF @w_signo = 'C'
				SELECT @w_trn = 253
			ELSE
				SELECT @w_trn = 264

			SELECT @w_trn_com = 264
		END
	END

	IF @w_cod_error = 0  --1
	BEGIN
		-- Cobrar comision por pago recibido
		IF @i_op_code = '560000' --OR @i_socio='S'
			SET @w_param_cau = 'CAUP2P'

		if @i_op_code='560050'
		begin
			SET @w_param_cau = 'CAUP2C'
			SET @w_param_cau_com = 'CACP2C'
			SET @w_par_mto_min  = 'MNTP2C'
			SET @w_par_porc_com = 'PERP2C' --1.5
			SET @w_par_com_min  = 'COMP2C'
			SET @w_concepto_com = 'COMISION PAGO RECIBIDO P2C'

			SELECT @w_causa_com = pa_char
			FROM  cobis..cl_parametro
			WHERE pa_producto = 'ATM'
			AND   pa_nemonico = @w_param_cau_com 

			--Calculo comision por servicio
			SELECT @w_porcentaje_comision = pa_money 
			FROM   cobis..cl_parametro 
			WHERE  pa_nemonico = @w_par_porc_com --porcentaje de comision
		
			SELECT @w_mto_com_min = pa_money
			FROM   cobis..cl_parametro 
			WHERE  pa_nemonico = @w_par_com_min  --monto minimo a cobrar por comision
		
			SELECT  @w_monto_limite = pa_money
			FROM   cobis..cl_parametro 
			WHERE  pa_nemonico = @w_par_mto_min  --si el pago es menor a este valor se cobra el minimo de comision

			IF @i_val <= isnull(@w_monto_limite,0)
				SET @w_mto_comision = isnull(@w_mto_com_min,0)
			ELSE
				SET @w_mto_comision = @i_val * (isnull(@w_porcentaje_comision,0) /100)
		end	

		--Causas contables
		SELECT @w_causa = pa_char
		FROM  cobis..cl_parametro
		WHERE pa_producto = 'ATM'
		AND   pa_nemonico = @w_param_cau 

		--Valida si los productos estan habilitados
		select @w_prod_habilitado_des = pm_estado
		from   cobis..cl_pro_moneda
		where  pm_producto = @i_producto
		and    pm_tipo     = 'R'
		and    pm_moneda   = @w_moneda	

		if @w_prod_habilitado_des != 'V'
			SET @w_cod_error = 235004   

		--Busca el secuencial transaccion
		exec @s_ssn = ADMIN...rp_ssn 1, 2

		SET @o_sec = @s_ssn

		--Busca el secuencial comision por error
		--exec @s_ssn_com = ADMIN...rp_ssn 1, 2

		IF @w_cod_error = 0 AND @s_ssn > 0 --AND @s_ssn_com > 0  --2
		BEGIN
			--Validacion ara los casos de socios que acumulan pagos con transaccion de servicio	
			if ISNULL(@i_tran_servicio,'N') = 'N'
			BEGIN
				BEGIN TRAN --Inicio Transaccion--------------------------------------------------------------------------------------------

				--Rutina para guardar en una cola  -- cob_remesas..sp_ejecuta_ndc 
				IF EXISTS (SELECT 1 FROM cobis..cl_parametro
						WHERE  pa_nemonico = 'CDPMEN'
						AND    pa_tipo = 'C'
						AND    pa_char = 'S')
				BEGIN
					--Monto del pago
					INSERT INTO cob_remesas..re_cre_deb
						(de_producto, de_srv, de_ofi, de_ssn, de_user, de_trn, de_ssn_corr, de_cta, de_val, de_cau, de_mon,  
						de_dep, de_alt, de_fecha, de_reverso, de_changeofi, de_nchq, de_concepto, de_ssn_branch, de_estado,
						de_cobsus, de_inmovi, de_idb, de_deb_lc_ctacte, de_exento, de_redondea, de_hora, de_nombre_sol)
					VALUES(@i_producto, @s_srv, @s_ofi, @s_ssn, @s_user, @w_trn, @t_ssn_corr, @i_cta_banco, @i_val, @w_causa, @w_moneda, 
						0, 20, @s_date, @w_corr, '', @i_referencia, @i_concepto, 0, 'PE',
						'N', 'N', 0, 'N', 'S', 'N', getdate(), @i_trace) 

					IF @@error <> 0
					begin
						if @@trancount > 0				
							ROLLBACK TRAN
					
						SET @w_cod_error = 50002  --El ingresando en la cola de pagos
					end

					if @i_op_code = '560050' AND @w_mto_comision > 0 AND @w_cod_error = 0  
					BEGIN
						--Monto de la comision
						INSERT INTO cob_remesas..re_cre_deb
							(de_producto, de_srv, de_ofi, de_ssn, de_user, de_trn, de_ssn_corr, de_cta, de_val, de_cau, de_mon,  
							de_dep, de_alt, de_fecha, de_reverso, de_changeofi, de_nchq, de_concepto, de_ssn_branch, de_estado,
							de_cobsus, de_inmovi, de_idb, de_deb_lc_ctacte, de_exento, de_redondea, de_hora, de_nombre_sol)
						VALUES(@i_producto, @s_srv, @s_ofi, @s_ssn, @s_user, @w_trn_com, @t_ssn_corr, @i_cta_banco, @w_mto_comision, @w_causa_com, @w_moneda, 
							0, 40, @s_date, @w_corr, '', @i_referencia, @i_concepto, 0, 'PE',
							'N', 'N', 0, 'N', 'S', 'N', getdate(), @i_trace) 

						IF @@error <> 0
						begin
							if @@trancount > 0				
								ROLLBACK TRAN
						
							SET @w_cod_error = 50002  --El ingresando en la cola de pagos
						end
					END
				END
				ELSE
				BEGIN
					--EJECUTAR NOTA CREDITO/DEBITO AL SOCIO
					exec @w_return = @w_rpc
						@s_srv        = @s_srv,
						@s_ofi        = @s_ofi,
						@s_ssn        = @s_ssn,
						@s_user       = @s_user,
						@t_trn        = @w_trn,	
						@t_ssn_corr   = @t_ssn_corr,
						@i_cta        = @i_cta_banco,
						@i_val        = @i_val,
						@i_cau        = @w_causa,
						@i_mon        = @w_moneda,
						@i_alt        = 20,
						@i_fecha      = @s_date,
						@i_concepto   = @i_concepto,
						@i_nchq       = @i_referencia,
						@i_reverso     = @w_corr,
						@i_solicitante = @i_trace
						
					if @w_return <> 0
					begin
						if @@trancount > 0				
							ROLLBACK TRAN
					
						SET @w_cod_error = @w_return
					end		

					if @i_op_code = '560050' AND @w_mto_comision > 0 AND @w_cod_error = 0  
					begin
						--EJECUTAR NOTA DEBITO COMISION MONETARIA
						exec @w_return    = @w_rpc
							@s_srv        = @s_srv,
							@s_ofi        = @s_ofi,
							@s_ssn        = @s_ssn,
							@s_user       = @s_user,
							@t_ssn_corr   = @t_ssn_corr,
							@t_trn        = @w_trn_com,	
							@i_cta        = @i_cta_banco,
							@i_val        = @w_mto_comision,
							@i_cau        = @w_causa_com,
							@i_mon        = @w_moneda,
							@i_alt        = 40,
							@i_fecha      = @s_date,
							@i_concepto   = @i_concepto,
							@i_nchq       = @i_referencia,
							@i_reverso    = @w_corr,
							@i_ssn_branch = 0,
							@i_solicitante = @i_trace

						IF @w_return <> 0
						begin
							if @@trancount > 0				
								ROLLBACK TRAN

							SET @w_cod_error = @w_return
						end	
					END		
				END
			END --Fin - if @i_tran_servicio = 'N'  
			ELSE
			BEGIN
				BEGIN TRAN --Inicio Transaccion--------------------------------------------------------------------------------------------
				if @w_corr = 'N'
				BEGIN
					-- registrar transaccion de servicio por el pago 		
					insert into cob_cuentas..cc_tran_servicio
						(ts_secuencial , ts_tipo_transaccion, ts_tsfecha,
						ts_usuario, ts_terminal, ts_oficina,
						ts_reentry, ts_origen, ts_cta_banco,
						ts_valor, ts_monto,ts_moneda, ts_oficina_cta, ts_prod_banc,ts_tasa,ts_correccion,
						ts_hora, ts_ced_ruc, ts_nro_factura,ts_remoto_ssn,ts_agente,ts_banco
						)
					values (@s_ssn, 2938, @s_date,
						'sa', @s_term, @s_ofi,
						'N', 'U', @i_cta_banco,
						@i_val,@i_val, @w_moneda, @s_ofi, @i_producto ,0, @w_corr,
						getdate(), @i_cedula_org, @i_telefono_org,0,@i_nom_cliente_benef,@w_banco
						)
						
					IF @@error <> 0
					begin
						if @@trancount > 0				
							ROLLBACK TRAN
					
						SET @w_cod_error = 40007
					end	

					IF @w_cod_error = 0
					BEGIN				
						-- registro para archivo de conciliacion pago recibido
						insert into cob_remesas..re_concilia_sw
							(cs_ente,cs_tipo,cs_origen,cs_destino,
							cs_cedula,cs_nombre,cs_monto,cs_referencia,
							cs_banco,cs_fecha,cs_hora,cs_comision,cs_estatus,cs_concepto
							)
						values(@i_cliente, 'P2PR', @i_telefono_org, @i_telefono_dest,
							@i_cedula_org, '', @i_val, @i_referencia,
							@i_banco, @s_date, getdate(), 0, 'L', @i_concepto
							)

						IF @@error <> 0
						begin
							if @@trancount > 0				
								ROLLBACK TRAN
						
							SET @w_cod_error = 50005
						end	
					END
				END
				ELSE
				BEGIN
					-- reversar transaccion de servicio 
					delete cob_cuentas..cc_tran_servicio
					where  ts_secuencial = @t_ssn_corr
					and    ts_tipo_transaccion = 2938

					IF @@rowcount = 0
					begin
						if @@trancount > 0				
							ROLLBACK TRAN
					
						SET @w_cod_error = 50003
					end		
				
					IF @w_cod_error = 0
					BEGIN
						--Elimina de la tabla para no hacer el pago
						DELETE cob_remesas..re_concilia_sw
						WHERE  cs_fecha = @s_date
						AND    cs_ente = @i_cliente
						AND    cs_tipo = 'P2PR'
						AND    cs_origen = @i_telefono_org
						AND    cs_destino = @i_telefono_dest
						AND    cs_cedula = @i_cedula_org
						AND    cs_nombre = ''
						AND    cs_monto = @i_val
						AND    cs_banco = @i_banco
						AND    cs_referencia = @i_referencia	
						AND    cs_estatus = 'L'		

						IF @@ERROR > 0
						BEGIN
							if @@trancount > 0				
								ROLLBACK TRAN
						
							SET @w_cod_error = 50003
						END		
					END	
				END		
			END 
	
			if @w_cod_error = 0
			BEGIN
				SET @w_cta_orig = ISNULL(@i_cta_orig, right('0000' + isnull(@i_banco, ''), 4) + isnull(@i_telefono_org, ''))

				-- Insercion en la tabla de pago de servicios
				insert into cob_remesas..re_pago_servicios
					(ps_cuenta_origen,   ps_prod_origen,           ps_fecha,
						ps_monto,           ps_cuenta_destino,        ps_prod_destino,
						ps_moneda,          ps_servicio,              ps_tipo,
						ps_contrato,        ps_cliente,               ps_secuencial)
				values (@w_cta_orig,         3,                       @s_date,
						@i_val,             @i_cta_banco,             @i_producto,
						@w_moneda,          @w_trn_p2p,               'T',
						null,               @i_nom_cliente_benef,     @s_ssn)
			
				if @@error != 0
				begin
					if @@trancount > 0				
						ROLLBACK TRAN

					SET @w_cod_error = 235014
				end	

				--Cobro de comision para socios que se cobra totalizado en batch recibidos uno a uno
				if @w_cod_error = 0
				BEGIN									
					IF ISNULL(@w_mto_com_socio,0) > 0
					BEGIN
						-- registro para archivo de conciliacion
						INSERT INTO cob_remesas..re_concilia_sw (
							cs_ente 		,cs_tipo		,cs_origen		,cs_destino
							,cs_cedula		,cs_nombre		,cs_monto		,cs_referencia
							,cs_banco		,cs_fecha		,cs_hora		,cs_comision
							,cs_estatus		,cs_concepto)
						VALUES (
							@i_cliente		,'P2PR'			,@i_telefono_org,@i_telefono_dest
							,@i_cedula_dest	,'-R1a1-'		,@i_val			,@i_referencia
							,@i_banco		,@s_date		,getdate()		,@w_mto_com_socio
							,'L'			,@i_concepto)
							
						IF @@error <> 0
						begin
							if @@trancount > 0				
								ROLLBACK TRAN

							SET @w_cod_error = 50005
						end				
					END 
				END 	    
			END		
		END --@w_cod_error = 0 --2
	END --@w_cod_error = 0 --1

	IF @w_cod_error = 0
	BEGIN
		SET @o_resultado = '00'
		SET @o_mensaje = 'TRANSACCION EXITOSA'
	END
	ELSE
	BEGIN
		SELECT @o_resultado = convert(varchar(10), er_error_red), 
				@o_mensaje = mensaje
		FROM   cob_atm..tm_errores_red
			INNER JOIN cobis..cl_errores
			on er_error_cobis = numero
		WHERE numero = @w_cod_error
		AND   er_srv = 'P2P'	

		IF ISNULL(@o_resultado, '') = ''
			SET @o_resultado = '12'
		
		IF ISNULL(@o_mensaje, '') = ''
			SET @o_mensaje = 'TRANSACCION INVALIDA'
	END

	--Ingresar informacion en la tabla re_pago_movil
	INSERT cob_remesas.dbo.re_pago_movil (
		pm_servicio		 ,pm_telefono_o		,pm_telefono_d	,pm_monto		,pm_banco	,pm_cta_banco	,pm_reverso			
		,pm_codigo_respuesta ,pm_error_cobis	,pm_msg			,pm_referencia	,pm_tipo	,pm_hora		,pm_ssn_branch	
		,pm_fecha			 ,pm_secuencial		,pm_signo   ,pm_cedula_o,   pm_cedula_d, pm_tipo_cedula_d--, pm_tipo_cedula_o
		)
	VALUES (
		@w_tipo			 ,@i_telefono_org	,@i_telefono_dest	,@i_val			,@i_banco	,@i_cta_banco  	, @w_corr
		,@o_resultado	 ,@w_cod_error		,@o_mensaje			,@w_referencia	,@w_tipo_pago	,getdate()		,@i_referencia
		,@s_date		 ,@o_sec		    ,@w_signo           ,@i_cedula_org  ,@i_cedula_dest, @i_tipo_ced_dest--, @i_tipo_ced_org
		)	 

	IF @@ERROR <> 0
	begin
		if @@trancount > 0				
			ROLLBACK TRAN

		SET @w_cod_error = @w_return
	END	

	IF @@trancount > 0	
		COMMIT TRAN

	SET @o_cod_error = @w_cod_error
END 

RETURN 0
go

IF OBJECT_ID('dbo.sp_atm_pago_socio_ws') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.sp_atm_pago_socio_ws >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.sp_atm_pago_socio_ws >>>'
go