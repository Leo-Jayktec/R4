use cob_cuentas
GO

IF OBJECT_ID('dbo.sp_atm_pago_ws') IS NOT NULL
BEGIN
	DROP PROCEDURE dbo.sp_atm_pago_ws 

	IF OBJECT_ID('dbo.sp_atm_pago_ws') IS NOT NULL
		PRINT '<<< FAILED DROPPING PROCEDURE dbo.sp_atm_pago_ws  >>>'
	ELSE
		PRINT '<<< DROPPED PROCEDURE dbo.sp_atm_pago_ws  >>>'
END
go

/****************************************************************/
/* NOMBRE ARCHIVO: sp_atm_pago_ws.sp                            */
/* BASE DE DATOS: cob_remesas (Sybase)                          */
/* PROCESA TRANSACCION MONETARIA PAGO ENTRANTE                  */
/* DE SOCIOS (Vuelto)                                           */
/* 05/09/2024  Emision inicial   Jayktec                        */
/****************************************************************/
/*  29/10/2024      Jayktec    Enviar Nombre del Socio          */
/*                             para crear la Tran. Servicio     */
/*  30/10/2024      Jayktec    Signo de la transaccion para     */
/*                           C2P cambiar a Debito               */
/*  06/11/2024      Jayktec  Manejo de reverso                  */
/*  23/09/2025      Jayktec  Registro en la tabla re_pago_movil */
/*                           por reversos                       */
/****************************************************************/

create proc sp_atm_pago_ws (
    @s_ssn                int,
	@s_srv                varchar(30)   = null,
	@s_lsrv               varchar(30)   = null,
	@s_user               varchar(30)   = null,
	@s_sesn               int           = null,
	@s_term               varchar(10),
	@s_date               datetime,
	@s_ofi                int,          -- Localidad origen transaccion 
	@s_rol                int           = 1,
	@s_org_err            char(1)       = null, -- Origen de error: [A], [S] 
	@s_error              int           = null,
	@s_sev                tinyint       = null,
	@s_msg                mensaje       = null,
	@s_org                char(1)       = null,
	@t_corr               char(1)       = 'N',
	@t_ejec               char(1)       = null,
	@t_ssn_corr           int           = null,
	@t_debug              char(1)       = 'N',
	@t_file               varchar(14)   = null,
	@t_from               varchar(32)   = null,
	@t_rty                char(1)       = 'N',
	@t_trn                int           = null,
	@i_operacion          char(1),
	@i_cliente			  int,
	@i_producto           smallint,
	@i_cta_banco          cuenta,
	@i_val                money,	
	@i_causa              varchar(3),
	@i_causa_com          varchar(3),
	@i_banco              int,
	@i_referencia         int,
	@i_concepto           descripcion,
	@i_ssn_branch         int,
	@i_tran_servicio      char(1) = 'N',
	@i_socio              char(1) = 'N',
	@i_trace			  varchar(64),
	@i_op_code            varchar(8),
	@i_cta_orig           varchar(20),
	@i_cedula_org	      numero,
	@i_telefono_org	      varchar(12),
	@i_cedula_dest	      numero,
	@i_telefono_dest	  varchar(12),	
	@i_nom_cliente_benef  varchar(64),
	@i_signo              char(1),
	@o_sec                int           = null out,
    @o_saldo_dispo        money         = null out,
    @o_saldo_conta        money         = null out,
    @o_ofi_cta            smallint      = null out,
    @o_manejo_cola        char(1)       = null out
)
as

declare
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
	@w_socio                    char(1),
	@w_nombre_benef             descripcion,
	@w_cuenta_int               int,
	@w_trn_p2p                  varchar(5),
	@w_telefono_socio			varchar(12),
	@w_telefono_orig	        varchar(12),	
	@w_telefono_dest	        varchar(12),	
	@w_mensaje            		varchar(200),
	@w_resultado          		char(10),
	@w_tipo 					varchar(20),
	@w_tipo_pago				varchar(20),
	@w_signo                    char(1),
	@w_referencia				varchar(15),
	--Comision
	@s_ssn_com                  int,
	@w_porcentaje_comision 	    money,
	@w_monto_limite             money,
	@w_mto_comision             money,
	@w_porc_com_socio			money,
	@w_mto_com_socio			money,
	@w_banco                    varchar(4),
	@w_mto_com_min				money,
	@w_mto_com_neg				money,
	@w_concepto_com				descripcion,
	@w_concepto_com_neg			descripcion,
	@w_par_mto_min   			VARCHAR(10),
	@w_par_porc_com   			VARCHAR(10),
	@w_par_com_min   			VARCHAR(10)	

--Inicializa Variables
SET @w_sp_name  = 'sp_atm_pago_ws'
SET @w_socio = ISNULL(@i_socio,'N')
SET @o_sec   = 0
SET @o_saldo_dispo = 0
SET @o_saldo_conta = 0
SET @o_ofi_cta = 0
SET @w_mto_comision = 0
SET @w_banco = '0' + convert(varchar(4), @i_banco)
SET @o_manejo_cola = 'N'
SET @w_cod_error = 0
SET @w_tipo = 'RECIBIDA'
SET @w_signo = 'C'
SET @w_referencia = RIGHT('000000000000' + CONVERT(VARCHAR(12),@i_referencia), 12)
SET @w_telefono_orig = '58' + RIGHT(ISNULL(@i_telefono_org,''),10)
SET @w_telefono_dest = '58' + RIGHT(ISNULL(@i_telefono_dest,''),10)

IF @t_corr = 'R'
	SET @w_telefono_orig = NULL

IF @i_tran_servicio = 'S'
	SET @w_trn_p2p = 'P2PSW'
ELSE
	SET @w_trn_p2p = 'P2P'

IF @i_operacion = 'I' ---Ingreso de Pago
BEGIN		
	--Busca el secuencial transaccion
	exec @s_ssn = ADMIN...rp_ssn 1, 2
			
	if isnull(@s_ssn, 0) = 0
		SET @w_cod_error = 40005
		
	SET @o_sec = @s_ssn

	--Busca el secuencial comision por error
	exec @s_ssn_com = ADMIN...rp_ssn 1, 2
			
	if isnull(@s_ssn_com, 0) = 0
		SET @w_cod_error = 40005

	IF @w_cod_error = 0
	BEGIN
		--Validar cuenta destino
		if @i_producto = 3
		BEGIN
			select @w_rpc = 'cob_cuentas..sp_ccndc_automatica_bv'

			select @w_cuenta_int = cc_ctacte,
				@w_moneda    = cc_moneda,
				@w_nombre_benef = cc_nombre,
				@o_ofi_cta = cc_oficina
			from  cob_cuentas..cc_ctacte
			where cc_cta_banco = @i_cta_banco
			and   cc_cliente = @i_cliente
			and   cc_estado    = 'A'
			and   cc_moneda    = 0

			if @@rowcount != 1
				SET @w_cod_error = 201004
			
			IF @i_signo = 'C'
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
				@o_ofi_cta = ah_oficina 
			from cob_ahorros..ah_cuenta
			where ah_cta_banco = @i_cta_banco
			and   ah_cliente = @i_cliente
			and   ah_estado    = 'A'
			and   ah_moneda    = 0
			
			if @@rowcount != 1
				SET @w_cod_error = 201004	

			IF @i_signo = 'C'
				SELECT @w_trn = 253
			ELSE
				SELECT @w_trn = 264

			SELECT @w_trn_com = 264
		END
		
		-- Cobrar comision por pago recibido
		IF @i_op_code = '560000' --OR @i_socio='S'
		BEGIN
			SET @w_par_mto_min  = 'MNTC2P'
			SET @w_par_porc_com = 'PERP2P' --0.3
			SET @w_par_com_min  = 'COMP2P'
			SET @w_concepto_com = 'COMISION PAGO RECIBIDO P2P'
			SET @w_tipo_pago = 'P2P'
		END	
		
		if @i_op_code='560050'
		begin
			SET @w_par_mto_min  = 'MNTP2C'
			SET @w_par_porc_com = 'PERP2C' --1.5
			SET @w_par_com_min  = 'COMP2C'
			SET @w_concepto_com = 'COMISION PAGO RECIBIDO P2C'
			SET @w_tipo_pago = 'P2C'
		end	

		if (@i_op_code='200030' OR @i_op_code='560009') --Anulacion o C2P 
		begin
			SET @w_par_mto_min  = 'MNTC2P'
			SET @w_par_porc_com = 'PERC2P' --2
			SET @w_par_com_min  = 'COMC2P'
			SET @w_concepto_com = 'COMISION PAGO RECIBIDO C2P'
			SET @w_tipo_pago = 'C2P'

			IF @i_op_code='560009'
			BEGIN
				SET @w_tipo = 'ENVIADA'	
				SET	@w_signo = 'D'	
			END
		end	

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

		IF @w_cod_error = 0
		BEGIN
			--Valida si los productos estan habilitados
			select @w_prod_habilitado_des = pm_estado
			from   cobis..cl_pro_moneda
			where  pm_producto = @i_producto
			and    pm_tipo     = 'R'
			and    pm_moneda   = @w_moneda	

			if @w_prod_habilitado_des != 'V'
				SET @w_cod_error = 235004   
		END

		--Validacion ara los casos de socios que acumulan pagos con transaccion de servicio	
		if ISNULL(@i_tran_servicio,'N') = 'N' AND @w_cod_error = 0
		BEGIN
			BEGIN TRAN --Inicio Transaccion--------------------------------------------------------------------------------------------

			--Rutina para guardar en una cola  -- cob_remesas..sp_ejecuta_ndc 
			if @w_socio = 'S' 
			BEGIN			
				IF EXISTS (SELECT * FROM cobis..cl_parametro
						WHERE  pa_nemonico = 'CDPMEN'
						AND    pa_tipo = 'C'
						AND    pa_char = 'S')
				BEGIN
					SET @o_manejo_cola = 'S'
					
					--Monto del pago
					INSERT INTO cob_remesas..re_cre_deb
						(de_producto, de_srv, de_ofi, de_ssn, de_user, de_trn, de_ssn_corr, de_cta, de_val, de_cau, de_mon,  
						de_dep, de_alt, de_fecha, de_reverso, de_changeofi, de_nchq, de_concepto, de_ssn_branch, de_estado,
						de_cobsus, de_inmovi, de_idb, de_deb_lc_ctacte, de_exento, de_redondea, de_hora, de_nombre_sol)
					VALUES(@i_producto, @s_srv, @s_ofi, @s_ssn, @s_user, @w_trn, @t_ssn_corr, @i_cta_banco, @i_val, @i_causa, @w_moneda, 
						0, 20, @s_date, @t_corr, '', @i_referencia, @i_concepto, @i_ssn_branch, 'PE',
						'N', 'N', 0, 'N', 'S', 'N', getdate(), @i_trace) 

					IF @@error <> 0
					begin
						if @@trancount > 0				
							ROLLBACK TRAN
					
						SET @w_cod_error = 50002  --El ingresando en la cola de pagos
					end
				END
				ELSE
				BEGIN
					--EJECUTAR NOTA CREDITO/DEBITO AL SOCIO
					exec @w_return = @w_rpc
						@s_srv        = @s_srv,
						@s_ofi        = @s_ofi,
						@s_ssn        = @s_ssn,
						@s_user       = @s_user,
						@t_ssn_corr   = @t_ssn_corr,
						@t_trn        = @w_trn,	
						@i_cta        = @i_cta_banco,
						@i_val        = @i_val,
						@i_cau        = @i_causa,
						@i_mon        = @w_moneda,
						@i_alt        = 20,
						@i_fecha      = @s_date,
						@i_concepto   = @i_concepto,
						@i_nchq       = @i_referencia,
						@i_reverso    = @t_corr,
						@i_ssn_branch = @i_ssn_branch,
						@i_solicitante = @i_trace
						
					if @w_return <> 0
					begin
						if @@trancount > 0				
							ROLLBACK TRAN
					
						SET @w_cod_error = @w_return
					end				
				END
			END
			ELSE
			BEGIN 	
				--EJECUTAR NOTA MONETARIA
				exec @w_return    = @w_rpc
					@s_srv        = @s_srv,
					@s_ofi        = @s_ofi,
					@s_ssn        = @s_ssn,
					@s_user       = @s_user,
					@t_ssn_corr   = @t_ssn_corr,
					@t_trn        = @w_trn,	
					@i_cta        = @i_cta_banco,
					@i_val        = @i_val,
					@i_cau        = @i_causa,
					@i_mon        = @w_moneda,
					@i_alt        = 20,
					@i_fecha      = @s_date,
					@i_concepto   = @i_concepto,
					@i_nchq       = @i_referencia,
					@i_reverso    = @t_corr,
					@i_ssn_branch = @i_ssn_branch,
					@i_solicitante = @i_trace

				IF @w_return <> 0
				begin
					if @@trancount > 0				
						ROLLBACK TRAN

					SET @w_cod_error = @w_return
				end	

				if (@i_op_code='560050' OR @i_op_code='560009' OR @i_op_code='200030')  AND @w_mto_comision > 0 AND @w_cod_error = 0--P2C -- OR @i_op_code='200030') --P2C o C2P 
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
						@i_cau        = @i_causa_com,
						@i_mon        = @w_moneda,
						@i_alt        = 40,
						@i_fecha      = @s_date,
						@i_concepto   = @i_concepto,
						@i_nchq       = @i_referencia,
						@i_reverso    = @t_corr,
						@i_ssn_branch = @i_ssn_branch,
						@i_solicitante = @i_trace

					IF @w_return <> 0
					begin
						if @@trancount > 0				
							ROLLBACK TRAN

						SET @w_cod_error = @w_return
					END	
				END
			END 
		END --Fin - if @i_tran_servicio = 'N'  
		ELSE
		BEGIN
			IF @w_cod_error = 0
			BEGIN
				BEGIN TRAN --Inicio Transaccion--------------------------------------------------------------------------------------------
			
				if @t_corr = 'R'
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
					AND    cs_banco = @w_banco
					AND    cs_referencia = @i_referencia	
					AND    cs_estatus = 'L'		

					IF @@ERROR <> 0
					begin
						if @@trancount > 0				
							ROLLBACK TRAN
					
						SET @w_cod_error = 50003
					end		
				END
				ELSE
				BEGIN
					-- registrar transaccion de servicio por el pago 		
					insert into cob_cuentas..cc_tran_servicio
						(ts_secuencial , ts_tipo_transaccion, ts_tsfecha,
						ts_usuario, ts_terminal, ts_oficina,
						ts_reentry, ts_origen, ts_cta_banco,
						ts_valor, ts_monto,ts_moneda, ts_oficina_cta, ts_prod_banc,ts_tasa,ts_correccion,
						ts_ssn_corr, ts_hora, ts_ced_ruc, ts_nro_factura,ts_remoto_ssn,ts_agente,ts_banco
						)
					values (@s_ssn, 2938, @s_date,
						'sa', @s_term, @s_ofi,
						'N', 'U', @i_cta_banco,
						@i_val,@i_val, @w_moneda, @s_ofi, @i_producto ,0, @t_corr,
						@t_ssn_corr, getdate(), @i_cedula_org, @i_telefono_org,@i_ssn_branch,@i_nom_cliente_benef,@i_banco
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
							@w_banco, @s_date, getdate(), 0, 'L', @i_concepto
							)

						IF @@error <> 0
						begin
							if @@trancount > 0				
								ROLLBACK TRAN
						
							SET @w_cod_error = 50005
						END	
					END
				END	
			END --IF @w_cod_error = 0
		END 
	END --@w_cod_error = 0

	if @t_corr <> 'R' AND @w_cod_error = 0
	begin
	    -- Insercion en la tabla de pago de servicios
	    insert into cob_remesas..re_pago_servicios
	           (ps_cuenta_origen,   ps_prod_origen,           ps_fecha,
	            ps_monto,           ps_cuenta_destino,        ps_prod_destino,
	            ps_moneda,          ps_servicio,              ps_tipo,
	            ps_contrato,        ps_cliente,               ps_secuencial)
	    values (@i_cta_orig,         3,                       @s_date,
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
		if @w_socio = 'S' 
		BEGIN
			SET @w_telefono_socio = '0' + RIGHT(@i_telefono_dest,10)
			
			SELECT @w_porc_com_socio = cc_comision
			FROM   cob_remesas..re_p2p_rec
			WHERE  cc_ente     = @i_cliente
			AND    cc_telefono = @w_telefono_socio
			
			SET @w_mto_com_socio = @i_val * (@w_porc_com_socio / 100)
			
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
					,@w_banco		,@s_date		,getdate()		,@w_mto_com_socio
					,'L'			,@i_concepto)
					
				IF @@error <> 0
				begin
					if @@trancount > 0				
						ROLLBACK TRAN

					SET @w_cod_error = 50005
				end				
			END 
		END 	    
    END		--Fin @t_corr <> 'R'
	
	--Actualizar tabla de pago movil
	IF @w_cod_error = 0
	BEGIN
		SET @w_resultado = '00'
		SET @w_mensaje = 'TRANSACCION EXITOSA'
	END
	ELSE
	BEGIN
		SELECT @w_resultado = convert(varchar(10), er_error_red), 
			@w_mensaje = mensaje
		FROM   cob_atm..tm_errores_red
			INNER JOIN cobis..cl_errores
			on er_error_cobis = numero
		WHERE numero = @w_cod_error
		AND   er_srv = 'P2P'	

		IF ISNULL(@w_resultado, '') = ''
			SET @w_resultado = '12'
		
		IF ISNULL(@w_mensaje, '') = ''
			SET @w_mensaje = 'TRANSACCION INVALIDA'
	END	

	--Ingresar informacion en la tabla re_pago_movil
	INSERT cob_remesas.dbo.re_pago_movil (
		pm_servicio		    ,pm_telefono_o		,pm_telefono_d	,pm_monto		,pm_banco	,pm_cta_banco	,pm_reverso			
		,pm_codigo_respuesta ,pm_error_cobis	,pm_msg			,pm_referencia	,pm_tipo	,pm_hora		,pm_ssn_branch	
		,pm_fecha			 ,pm_secuencial		,pm_signo       ,pm_cedula_o,   pm_cedula_d
		)
	VALUES (--@w_tipo_pago 
		@w_tipo_pago        ,@w_telefono_orig	,@w_telefono_dest	,@i_val			,@w_banco	,@i_cta_banco  	, @t_corr
		,@w_resultado	 	,@w_cod_error		,@w_mensaje			,@w_referencia	,@w_tipo	,getdate()		,@i_ssn_branch
		,@s_date			 ,@o_sec			,@w_signo           ,@i_cedula_org,  @i_cedula_dest )	

	if @@ERROR != 0
	begin
		if @@trancount > 0				
			ROLLBACK TRAN

		SET @w_cod_error = 235014
	end	

	IF @@trancount > 0	
		COMMIT TRAN
	    
	-- Calcular el saldo --
	if @i_producto = 3
	begin
		 exec @w_return = cob_cuentas..sp_calcula_saldo
		      @t_from             = @w_sp_name,
		      @i_cuenta           = @w_cuenta_int,
		      @i_fecha            = @s_date,
		      @i_ofi              = @o_ofi_cta,
		      @i_transaccion      = @w_trn,    
		      @i_causa            = @i_causa,   
	          @i_cobsus           = @s_user,
	          @o_saldo_para_girar = @o_saldo_dispo out,
	          @o_saldo_contable   = @o_saldo_conta out	      
	 end
	
	if @i_producto = 4
	begin
	    exec @w_return = cob_ahorros..sp_ahcalcula_saldo
	         @t_debug            = null,
	         @t_file             = null,
	         @t_from             = @w_sp_name,
	         @i_cuenta           = @w_cuenta_int,
	         @i_fecha            = @s_date,
	         @i_ofi              = @o_ofi_cta,
	         @o_saldo_para_girar = @o_saldo_dispo out,
	         @o_saldo_contable   = @o_saldo_conta out        
	end		

END 

RETURN @w_cod_error
go

IF OBJECT_ID('dbo.sp_atm_pago_ws') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.sp_atm_pago_ws >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.sp_atm_pago_ws >>>'
go