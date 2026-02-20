UPDATE xxint.xxint_ap_i017_kyriba_payments_ack_stg
SET
    status = 'Manually_Voided'
WHERE
    reference IN ( '2331', '2329', '2394' );

COMMIT;