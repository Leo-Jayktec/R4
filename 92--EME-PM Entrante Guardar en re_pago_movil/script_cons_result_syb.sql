declare @w_return        varchar(2),
        @w_referencia    varchar(200)

execute cob_remesas..sp_rem_cons_result_p2p 
    @i_telefono_org        = '04120000000', --'04129196677',
    @i_telefono_des        = '04127283209', --'04222513996',
    @i_referencia        = '000220243725',
    @i_banco            = '0177',
    @i_fecha_tran        = '09/02/2025',    
    @o_resultado        = @w_return    output,
    @o_referencia    = @w_referencia    output


Select @w_return Retorno, @w_referencia Referencia