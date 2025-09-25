USE cob_cuentas
go
IF OBJECT_ID('dbo.sp_atm_pago_ws') IS NOT NULL
BEGIN
    DROP PROCEDURE dbo.sp_atm_pago_ws
    IF OBJECT_ID('dbo.sp_atm_pago_ws') IS NOT NULL
        PRINT '<<< FAILED DROPPING PROCEDURE dbo.sp_atm_pago_ws >>>'
    ELSE
        PRINT '<<< DROPPED PROCEDURE dbo.sp_atm_pago_ws >>>'
END
go
/************************************************************/
/* NOMBRE ARCHIVO: sp_atm_pago_ws.sp                        */
/* BASE DE DATOS: cob_remesas (Sybase)                      */
/* PROCESA TRANSACCION MONETARIA PAGO ENTRANTE              */
/* DE SOCIOS (Vuelto)                                       */
/* 05/09/2024  Emision inicial   Jayktec                    */
/************************************************************/
/*  29/10/2024      Jayktec    Enviar Nombre del Socio      */
/*                             para crear la Tran. Servicio */
/*  30/10/2024      Jayktec    Signo de la transaccion para */
/*                           C2P cambiar a Debito           */
/*  06/11/2024      Jayktec  Manejo de reverso              */
/************************************************************/

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
	--Comision
	@s_ssn_com                  int,
	@w_porcentaje_comision 	    money,
	@w_comision_p2c             money,
	@w_comision_p2p             money,
	@w_monto_limite             money,
	@w_mto_comision             money,
	@w_concepto_com             descripcion,
	@w_banco                    varchar(4)

--Inicializa Variables
SET @w_sp_name  = 'sp_atm_pago_ws'
SET @w_socio = ISNULL(@i_socio,'N')
SET @o_sec   = 0
SET @o_saldo_dispo = 0
SET @o_saldo_conta = 0
SET @o_ofi_cta = 0
SET @w_mto_comision = 0
SET @w_banco = convert(varchar(4), @i_banco)
SET @o_manejo_cola = 'N'

IF @i_tran_servicio = 'S'
	SET @w_trn_p2p = 'P2PSW'
ELSE
	SET @w_trn_p2p = 'P2P'

IF @i_operacion = 'I' ---Ingreso de Pago
BEGIN		
	--Busca el secuencial transaccion
	exec @s_ssn = ADMIN...rp_ssn 1, 2
			
	if isnull(@s_ssn, 0) = 0
		return 40005
		
	SET @o_sec = @s_ssn
	
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
		and   cc_estado    = 'A'
		and   cc_moneda    = 0

		if @@rowcount != 1
			return 201004
		
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
		and   ah_estado    = 'A'
		and   ah_moneda    = 0
		
		if @@rowcount != 1
			return 201004	

		IF @i_signo = 'C'
			SELECT @w_trn = 253
		ELSE
			SELECT @w_trn = 264
		SELECT @w_trn_com = 264
	END
		
	-- Cobrar comision por pago recibido
	if (@i_op_code='560050' OR @i_op_code='560009') --P2C o C2P 
	begin
		SELECT @w_porcentaje_comision = isnull(pa_money,0) 
		FROM   cobis..cl_parametro 
        WHERE  pa_nemonico='PERP2C'
	   
	    SELECT @w_comision_p2c = isnull(pa_money,0) 
	    FROM   cobis..cl_parametro 
        WHERE  pa_nemonico='COMP2C'
	   
	    SELECT  @w_monto_limite = isnull(pa_money,0)  
	    FROM   cobis..cl_parametro 
	    WHERE  pa_nemonico='MNTP2C'
	
	    IF @i_val <= @w_monto_limite
	   		SET @w_mto_comision = @w_comision_p2c 
	    ELSE
	   		SET @w_mto_comision = (@i_val * @w_porcentaje_comision) /100
/*
		IF EXISTS( SELECT 1 
		           FROM   cob_remesas..re_p2p_rec 
	               WHERE  cc_cta_banco = @i_cta_banco)
	       SET @w_mto_comision = 0
	       */
		SET @w_concepto_com = 'COMISION PAGO RECIBIDO C2P'
	end				

	IF @i_op_code = '560000' --OR @i_socio='S'
	BEGIN
		SELECT @w_porcentaje_comision = isnull(pa_money, 0)
		FROM   cobis..cl_parametro
		WHERE  pa_nemonico = 'PERP2P'
	
		SELECT @w_comision_p2p = isnull(pa_money, 0)
		FROM   cobis..cl_parametro
		WHERE  pa_nemonico = 'COMP2P'
	
		SELECT @w_monto_limite = isnull(pa_money, 0)
		FROM   cobis..cl_parametro
		WHERE  pa_nemonico = 'MNTP2P'
	
		IF @i_val <= @w_monto_limite
			SET @w_mto_comision = @w_comision_p2p
		ELSE
			SET @w_mto_comision = (@i_val * @w_porcentaje_comision) / 100
			
		SET @w_concepto_com = 'COMISION PAGO RECIBIDO P2P'
	END

	if @i_tran_servicio = 'N'
	BEGIN
		--Valida si los productos estan habilitados
		select @w_prod_habilitado_des = pm_estado
		from   cobis..cl_pro_moneda
		where  pm_producto = @i_producto
		and    pm_tipo     = 'R'
		and    pm_moneda   = @w_moneda	

		if @w_prod_habilitado_des != 'V'
			return  235004   

		begin tran --Inicio Transaccion--------------------------------------------------------------------------------------------

		--Rutina para guardar en una cola  -- cob_remesas..sp_ejecuta_ndc 
		if ISNULL(@w_socio,'N') = 'S' 
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
						rollback tran
				
					return 50002  --El ingresando en la cola de pagos
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
						rollback tran
				
					return @w_return
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
					rollback tran

				return @w_return
			end	

			if (@i_op_code='560050' OR @i_op_code='560009' OR @i_op_code='200030') --P2C o C2P 
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
						rollback tran

					return @w_return
				end	
			END
		END 
	END --Fin - if @i_tran_servicio = 'N'  
	ELSE
	BEGIN
		begin tran --Inicio Transaccion--------------------------------------------------------------------------------------------
	
		if @t_corr = 'R'
		BEGIN
			-- reversar transaccion de servicio 
			delete cob_cuentas..cc_tran_servicio
		    where  ts_secuencial = @t_ssn_corr
		    and    ts_tipo_transaccion = 2938

			IF @@rowcount = 0
			begin
				if @@trancount > 0				
					rollback tran
			
				return 50003
			end		
		END
		ELSE
		BEGIN
			-- registrar transaccion de servicio 		
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
				   @t_ssn_corr, getdate(), @i_cedula_org, '58' + right(@i_telefono_org,10),@i_ssn_branch,@i_nom_cliente_benef,@i_banco
				   )
				   
			IF @@error <> 0
			begin
				if @@trancount > 0				
					rollback tran
			
				return 40007
			end	
								   
			-- registro para archivo de conciliacion
			insert into cob_remesas..re_concilia_sw
				(cs_ente,cs_tipo,cs_origen,cs_destino,
				cs_cedula,cs_nombre,cs_monto,cs_referencia,
				cs_banco,cs_fecha,cs_hora,cs_comision,cs_estatus,cs_concepto
				)
			values(@i_cliente, 'P2PR', @i_telefono_org, @i_telefono_dest,
				@i_cedula_org, '', @i_val, @i_referencia,
			    convert(char(4),@i_banco), @s_date, getdate(), 0, 'L', @i_concepto
				)

			IF @@error <> 0
			begin
				if @@trancount > 0				
					rollback tran
			
				return 50005
			end	
		END	
	END 

	if @t_corr <> 'R'
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
				rollback tran

			return 235014
	    end	

	    --Ingreso de comision para socios que se cobra totalizado en batch recibidos uno a uno
		IF EXISTS(SELECT 1 --@w_mto_comision = cc_comision
		          FROM cob_remesas..re_p2p_rec
		          WHERE cc_ente = @i_cliente) AND @i_val > 0 AND @i_tran_servicio = 'N'
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
				,@w_banco		,@s_date		,getdate()		,@w_mto_comision
				,'L'			,@i_concepto)
				
			IF @@error <> 0
			begin
				if @@trancount > 0				
					rollback tran

				return 50005
			end				
		END  	    
    END
	ELSE
	BEGIN
		DELETE cob_remesas..re_concilia_sw
		WHERE  cs_fecha = @s_date
		AND    cs_monto = @i_val
		AND    cs_banco = @w_banco
		AND    cs_referencia = @i_referencia	
	END	
    
	if @@trancount > 0				 
		commit tran

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
	          
		IF @w_return <> 0
			return @w_return 	      
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
	         
		IF @w_return <> 0
			return @w_return 	         
	end		

END 

RETURN 0
go
EXEC sp_procxmode 'dbo.sp_atm_pago_ws', 'unchained'
go
IF OBJECT_ID('dbo.sp_atm_pago_ws') IS NOT NULL
    PRINT '<<< CREATED PROCEDURE dbo.sp_atm_pago_ws >>>'
ELSE
    PRINT '<<< FAILED CREATING PROCEDURE dbo.sp_atm_pago_ws >>>'
go
