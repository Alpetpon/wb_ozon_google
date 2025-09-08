<?

include 'sql.php'; //загрузка параметров MySQL

// блок получения айди клиента и апи ключа из входящего POST JSON

$data = json_decode(file_get_contents('php://input'), true);

$cabinet = $data[cabinet];          // идентификатор клиента - строка в базе

$clientID = $data[clientID];        // Client ID Ozon
$apiKey = $data[apiKey];            // Api key Ozon

$df = $data[df];
$de = $data[de];


get_voronka($df , $de);




function get_a($s_date, $nd_date, $ofs){
 
global $clientID, $apiKey;

$url = "https://api-seller.ozon.ru/v1/analytics/data";

$payload = [
    'date_from' => $s_date ,
    'date_to' =>   $nd_date, 
    'metrics' => [ 
        
        "ordered_units", //заказано товаров.
        "revenue",  // заказано на сумму,
        "session_view", //— всего сессий. Считаются уникальные посетители.
        "session_view_pdp", //— сессии с показом на карточке товара. Считаются уникальные посетители, которые просмотрели карточку товара.
        "conv_tocart_pdp", //— конверсия в корзину из карточки товара.
        "cancellations", //— отменено товаров.
        "returns", //— возвращено товаров.
        "position_category", // — позиция в поиске и категории.
           

        "hits_view_search", // — показы в поиске и в категории.

        "session_view_search", //— сессии с показом в поиске или в каталоге. Считаются уникальные посетители с просмотром в поиске или каталоге.

        "conv_tocart_search", //— конверсия в корзину из поиска или категории.
        "conv_tocart", //— общая конверсия в корзину.
        "hits_tocart_search", //— в корзину из поиска или категории.
        "hits_view"  //— всего показов.    
    ],
    
    'dimension' => [
        "day",
        "sku"
        
    ],
    
    'filters' => [],
    'sort' => [
        
        ['key' => 'day',
        'day' => 'ASC']
          
    ], 
    
    'limit' => 1000,
    'offset' => $ofs
 
];

$body = json_encode($payload);

$headers = [
        'Content-Type: application/json',
        'Client-Id:'.$clientID.'',
        'Api-Key:'.$apiKey.''
        ];  


$ch = curl_init();   

    curl_setopt($ch, CURLOPT_URL, $url);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'POST');
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
    curl_setopt($ch, CURLOPT_POSTFIELDS, $body);
    curl_setopt($ch, CURLINFO_HTTP_CODE, true);
    curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
    
    $response = curl_exec($ch);
    
   
    $status =  curl_getinfo($ch, CURLINFO_HTTP_CODE);

    if ($status==200){                                  // тест статуса ответа API
        
         $array = json_decode($response, true);
         
        
         
         return $array;

    }

}


function get_voronka($start_date_s, $end_date_s) {
    
  global $cabinet, $link;
  
  $query = "DELETE from voronka  WHERE (cabinet='".$cabinet."' and dat >='".$start_date_s."'  and dat <='".$end_date_s."')";
  $a = mysqli_query($link,$query);
    
  $ofs = 0;
  $next_page = true;
  
  $result_arr = [];
  
  
  while ($next_page==true){
      
    $data = get_a($start_date_s, $end_date_s, $ofs);
      
    $res = $data['result']['data'];
    
      
     
      
      if ($res!=""){
          
       
       
       if (count($res) == 1000){
           
           $ofs = $ofs+1000;
           sleep(10);
         
           
       } else {
           
             $next_page= false;
             
       }
       
       
       foreach ($res As $elem){
           
           $dimensions = $elem['dimensions'];
           
          
           
           $dat = $dimensions[0]['id'];
           $sku = $dimensions[1]['id'];
           $nam = $dimensions[1]['name'];
           
           $empt = "";
           
           $metrics = $elem['metrics'];
           
           
           
           $ordered_units = $metrics[0];   //заказано товаров.
           $revenue= str_replace('.', ',' , $metrics[1]);  // заказано на сумму,
           $session_view= str_replace('.', ',' , $metrics[2]); //— всего сессий. Считаются уникальные посетители.
           $session_view_pdp= str_replace('.', ',' , $metrics[3]); //— сессии с показом на карточке товара. Считаются уникальные посетители, которые просмотрели карточку товара.
           $conv_tocart_pdp= str_replace('.', ',' , $metrics[4]); //— конверсия в корзину из карточки товара.
           $cancellations= str_replace('.', ',' , $metrics[5]); //— отменено товаров.
           $returns= str_replace('.', ',' , $metrics[6]); //— возвращено товаров.
           $position_category= str_replace('.', ',' , $metrics[7]); // — позиция в поиске и категории.
           $hits_view_search= str_replace('.', ',' , $metrics[8]); // — показы в поиске и в категории.
           $session_view_search= str_replace('.', ',' , $metrics[9]); //— сессии с показом в поиске или в каталоге. Считаются уникальные посетители с просмотром в поиске или каталоге.
           $conv_tocart_search= str_replace('.', ',' , $metrics[10]); //— конверсия в корзину из поиска или категории.
           $conv_tocart= str_replace('.', ',' , $metrics[11]); //— общая конверсия в корзину.
           $hits_tocart_search= str_replace('.', ',' , $metrics[12]); //— в корзину из поиска или категории.
           $hits_view= str_replace('.', ',' , $metrics[13]); //— всего показов.
           
          
           
             //  запись в базу товаров 
                 
            $query = "INSERT INTO voronka (cabinet, dat, sku, nam, empt, ordered_units, revenue, session_view, session_view_pdp, conv_tocart_pdp, cancellations, returns, position_category, hits_view_search, session_view_search, conv_tocart_search, conv_tocart, hits_tocart_search, hits_view )  VALUES ('".$cabinet."', '".$dat."', '".$sku."', '".$nam."', '".$empt."', '".$ordered_units."', '".$revenue."', '".$session_view."', '".$session_view_pdp."', '".$conv_tocart_pdp."', '".$cancellations."', '".$returns."', '".$position_category."', '".$hits_view_search."', '".$session_view_search."', '".$conv_tocart_search."', '".$conv_tocart."', '".$hits_tocart_search."', '".$hits_view."')"; 
                
            $a = mysqli_query($link,$query);
           
           
           
       }
       

      } else {
          
          $next_page= false;
      }
      
  }
  
    
}





?>