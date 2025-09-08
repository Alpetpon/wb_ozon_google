<?

include 'sql.php';

$data = json_decode(file_get_contents('php://input'), true);

$cabinet = $data['cabinet'];

$table = $data['table'];

//$df = $_POST['df'];

//$dt = $_POST['dt'];

$query = "SELECT * from ".$table." WHERE cabinet='".$cabinet."'";

$result = mysqli_query($link,$query);

while($row = $result->fetch_assoc()) {
    $myArray[] = $row;
}

echo json_encode($myArray);



?>