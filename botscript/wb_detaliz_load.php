<?

include 'sql.php';

$data = json_decode(file_get_contents('php://input'), true);

$cabinet = $data[cabinet];
$apiKey = $data[apiKey];
$date_from = $data[df];
$date_to = $data[dt];




$limit = 100000;
$rrdid = 0;
$need_parsing = true;


$query = "DELETE from detaliz_wb WHERE (cabinet='".$cabinet."' AND date_from='".$date_from."' AND date_to='".$date_to."')";
$a = mysqli_query($link,$query);

$headers = [
        'Content-Type: application/json',
        'Authorization:'.$apiKey.''
      
        ]; 

while ($need_parsing == true) {

    $ch = curl_init();
    
    $url = 'https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod?dateFrom='.$date_from.'T00:00:00&dateTo='.$date_to.'T23:59:59&limit='.$limit.'&rrdid='.$rrdid.'';
    
    echo $url;
    
    $ch = curl_init();   
    curl_setopt($ch, CURLOPT_URL, $url);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
    curl_setopt($ch, CURLINFO_HTTP_CODE, true);
    curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
    
    $response = curl_exec($ch);
    
    
   
    
    $status =  curl_getinfo($ch, CURLINFO_HTTP_CODE);
    
    $array = json_decode($response, true);
    
    if ($status==200) {
   
        if (count($array)>0) {
    
            
            foreach ($array As $realiz) {
            
                         
                         $realizationreport_id = $realiz['realizationreport_id'];
                         $date_from = $realiz['date_from'];
                         $date_to = $realiz['date_to'];
                         $create_dt = $realiz['create_dt'];
                         $currency_name = $realiz['currency_name'];
                         $suppliercontract_code = $realiz['suppliercontract_code'];
                         $rrd_id= $realiz['rrd_id'];
                         $gi_id = $realiz['gi_id'];
                         $subject_name = $realiz['subject_name'];
                         $nm_id = $realiz['nm_id'];
                         $brand_name = $realiz['brand_name'];
                         $sa_name = $realiz['sa_name'];
                         $ts_name = $realiz['ts_name'];
                         $barcode = $realiz['barcode'];
                         $doc_type_name = $realiz['doc_type_name'];
                         $quantity = $realiz['quantity'];
                         
                         $retail_price = str_replace('.',',', $realiz['retail_price']);
                         
                         $retail_amount = str_replace('.',',', $realiz['retail_amount']);
                         
                         $sale_percent = str_replace('.',',', $realiz['sale_percent']);
                         
                         $commission_percent = str_replace('.',',', $realiz['commission_percent']);
                         
                         
                         $office_name = $realiz['office_name'];
                         $supplier_oper_name = $realiz['supplier_oper_name'];
                         $order_dt = $realiz['order_dt'];
                         $sale_dt = $realiz['sale_dt'];
                         $rr_dt = $realiz['rr_dt'];
                         $shk_id = $realiz['shk_id'];
                         
                         $retail_price_withdisc_rub = str_replace('.',',', $realiz['retail_price_withdisc_rub']);
                         
                         $delivery_amount = str_replace('.',',', $realiz['delivery_amount']);
                         
                         $return_amount = str_replace('.',',', $realiz['return_amount']);
                         
                         
                         $delivery_rub = str_replace('.',',', $realiz['delivery_rub']);
                         
                         $gi_box_type_name = $realiz['gi_box_type_name'];
                         
                         $product_discount_for_report = str_replace('.',',', $realiz['product_discount_for_report']);
                         
                         $supplier_promo = str_replace('.',',', $realiz['supplier_promo']);
                         
                         $rid = $realiz['rid'];
                         
                         $ppvz_spp_prc = str_replace('.',',', $realiz['ppvz_spp_prc']);
                         
                         $ppvz_kvw_prc_base = str_replace('.',',', $realiz['ppvz_kvw_prc_base']);
                         $ppvz_kvw_prc = str_replace('.',',', $realiz['ppvz_kvw_prc']);
                         $sup_rating_prc_up = str_replace('.',',', $realiz['sup_rating_prc_up']);
                         $is_kgvp_v2 = str_replace('.',',', $realiz['is_kgvp_v2']);
                         $ppvz_sales_commission = str_replace('.',',', $realiz['ppvz_sales_commission']);
                         $ppvz_for_pay = str_replace('.',',', $realiz['ppvz_for_pay']);
                         $ppvz_reward = str_replace('.',',', $realiz['ppvz_reward']);
                         $acquiring_fee = str_replace('.',',', $realiz['acquiring_fee']);
                         
                         
                         $acquiring_bank = $realiz['acquiring_bank'];
                         
                         $ppvz_vw = str_replace('.',',', $realiz['ppvz_vw']);
                         $ppvz_vw_nds = str_replace('.',',', $realiz['ppvz_vw_nds']);
                         $ppvz_office_id = str_replace('.',',', $realiz['ppvz_office_id']);
                         $ppvz_office_name = $realiz['ppvz_office_name'];
                         
                         $ppvz_supplier_id = $realiz['ppvz_supplier_id'];
                         $ppvz_supplier_name = $realiz['ppvz_supplier_name'];
                         $ppvz_inn = $realiz['ppvz_inn'];
                         $declaration_number = $realiz['declaration_number'];
                         $bonus_type_name = $realiz['bonus_type_name'];
                         $sticker_id = $realiz['sticker_id'];
                         $site_country = $realiz['site_country'];
                         
                         $penalty = str_replace('.',',', $realiz['penalty']);
                         
                         $additional_payment = str_replace('.',',', $realiz['additional_payment']);
                         $rebill_logistic_cost = str_replace('.',',', $realiz['rebill_logistic_cost']);
                         $rebill_logistic_org = $realiz['rebill_logistic_org'];
                         $kiz = $realiz['kiz'];
                         
                         $storage_fee = str_replace('.',',', $realiz['storage_fee']);
                         $deduction = str_replace('.',',', $realiz['deduction']);
                         $acceptance = str_replace('.',',', $realiz['acceptance']);
                         
                         $srid = $realiz['srid'];
                         $report_type = $realiz['report_type'];
         
         
               
                         $query = "INSERT INTO detaliz_wb (cabinet, realizationreport_id, date_from, date_to, create_dt, currency_name, suppliercontract_code, rrd_id, gi_id, subject_name, nm_id, brand_name, sa_name, ts_name, barcode, doc_type_name, quantity, retail_price, retail_amount, sale_percent, commission_percent, office_name, supplier_oper_name, order_dt, sale_dt, rr_dt, shk_id, retail_price_withdisc_rub, delivery_amount, return_amount, delivery_rub, gi_box_type_name, product_discount_for_report, supplier_promo, rid, ppvz_spp_prc, ppvz_kvw_prc_base, ppvz_kvw_prc, sup_rating_prc_up, is_kgvp_v2, ppvz_sales_commission, ppvz_for_pay, ppvz_reward, acquiring_fee, acquiring_bank, ppvz_vw, ppvz_vw_nds, ppvz_office_id, ppvz_office_name, ppvz_supplier_id, ppvz_supplier_name, ppvz_inn, declaration_number, bonus_type_name, sticker_id, site_country, penalty, additional_payment, rebill_logistic_cost, rebill_logistic_org, kiz, storage_fee, deduction, acceptance, srid, report_type) VALUES ('".$cabinet."', '".$realizationreport_id."', '".$date_from."', '".$date_to."', '".$create_dt."', '".$currency_name."', '".$suppliercontract_code."', '".$rrd_id."', '".$gi_id."', '".$subject_name."', '".$nm_id."', '".$brand_name."', '".$sa_name."', '".$ts_name."', '".$barcode."', '".$doc_type_name."', '".$quantity."', '".$retail_price."', '".$retail_amount."', '".$sale_percent."', '".$commission_percent."', '".$office_name."', '".$supplier_oper_name."', '".$order_dt."', '".$sale_dt."', '".$rr_dt."', '".$shk_id."', '".$retail_price_withdisc_rub."', '".$delivery_amount."', '".$return_amount."', '".$delivery_rub."', '".$gi_box_type_name."', '".$product_discount_for_report."', '".$supplier_promo."', '".$rid."', '".$ppvz_spp_prc."', '".$ppvz_kvw_prc_base."', '".$ppvz_kvw_prc."', '".$sup_rating_prc_up."', '".$is_kgvp_v2."', '".$ppvz_sales_commission."', '".$ppvz_for_pay."', '".$ppvz_reward."', '".$acquiring_fee."', '".$acquiring_bank."', '".$ppvz_vw."', '".$ppvz_vw_nds."', '".$ppvz_office_id."', '".$ppvz_office_name."', '".$ppvz_supplier_id."', '".$ppvz_supplier_name."', '".$ppvz_inn."', '".$declaration_number."', '".$bonus_type_name."', '".$sticker_id."', '".$site_country."', '".$penalty."', '".$additional_payment."', '".$rebill_logistic_cost."', '".$rebill_logistic_org."', '".$kiz."', '".$storage_fee."', '".$deduction."', '".$acceptance."', '".$srid."', '".$report_type."')";
                        
                         $a = mysqli_query($link,$query);
             
           
            
            }
            
            
            
            
             if(count($array)==$limit) {
            
             $rrdid = $array[$limit-1][rrd_id];
          
             sleep(60);
            
             } else { $need_parsing = false;}
         
         
        
        } else { $need_parsing = false;}
        
    
    } else { $need_parsing = false;}
       

}




?>