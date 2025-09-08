<?



include 'sql.php'; //загрузка параметров MySQL


$data = json_decode(file_get_contents('php://input'), true);

$user_id =$data[user_id];          // идентификатор клиента - строка в базе
$api_key = $data[api_key];    
$cabinet = $data[cabinet];  

$query = "DELETE from bot_apikey_wb  WHERE (cabinet='".$cabinet."' and user_id='".$user_id."')";	//удаление кабинета, если он уже есть
$a = mysqli_query($link,$query);

$query = "INSERT INTO bot_apikey_wb (user_id, apikey, cabinet)  VALUES ('".$user_id."', '".$api_key."', '".$cabinet."')"; 
                
$a = mysqli_query($link,$query);




?>