<?

include 'sql.php'; //загрузка параметров MySQL


$data = json_decode(file_get_contents('php://input'), true);


$cabinet = $data[cabinet];  

$user_id = $data[user_id];  

$client_id = $data[client_id];  
$oz_api = $data[oz_api];  
$oz_perf = $data[oz_perf];  
$oz_secret = $data[oz_secret];  



$query = "DELETE from bot_apikey_ozon  WHERE (cabinet='".$cabinet."' and user_id='".$user_id."')";	//удаление кабинета, если он уже есть
$a = mysqli_query($link,$query);

$query = "INSERT INTO bot_apikey_ozon (user_id, client_id, oz_api,  oz_perf, oz_secret, cabinet)  VALUES ('".$user_id."', '".$client_id."', '".$oz_api."', '".$oz_perf."','".$oz_secret."', '".$cabinet."')"; 
                
$a = mysqli_query($link,$query);




?>