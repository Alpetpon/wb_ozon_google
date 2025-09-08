<?

include 'sql.php';

$data = json_decode(file_get_contents('php://input'), true);

$cabinet = $data['cabinet'];

$table = $data['table'];

$df = $data['df'];

$de = $data['de'];


if ($table == 'tovar'){

$query = "SELECT * from tovar WHERE cabinet='".$cabinet."'";

$result = mysqli_query($link,$query);

while($row = $result->fetch_assoc()) {
    $myArray[] = $row;
}

echo json_encode($myArray);

} 



if ($table == 'detaliz'){       // даты





$query = "SELECT * from detaliz WHERE (cabinet='".$cabinet."' and c1 >='".$df."'  and c1 <='".$de."')";





$result = mysqli_query($link,$query);

while($row = $result->fetch_assoc()) {
    $myArray[] = $row;
}

echo json_encode($myArray);


} 



if ($table == 'prices'){       // даты

$query = "SELECT * from prices WHERE (cabinet='".$cabinet."' and dat >='".$df."'  and dat <='".$de."')";

$result = mysqli_query($link,$query);

while($row = $result->fetch_assoc()) {
    $myArray[] = $row;
}

echo json_encode($myArray);

} 



if ($table == 'stat_rk'){       // даты

$query = "SELECT * from stat_rk WHERE (cabinet='".$cabinet."' and date >='".$df."'  and date <='".$de."')";

$result = mysqli_query($link,$query);

while($row = $result->fetch_assoc()) {
    $myArray[] = $row;
}

echo json_encode($myArray);

} 

if ($table == 'voronka'){       // даты

$query = "SELECT * from voronka WHERE (cabinet='".$cabinet."' and dat >='".$df."'  and dat <='".$de."')";

$result = mysqli_query($link,$query);

while($row = $result->fetch_assoc()) {
    $myArray[] = $row;
}

echo json_encode($myArray);

} 


if ($table == 'stocks'){       // даты

$query = "SELECT * from stocks WHERE (cabinet='".$cabinet."' and dat >='".$df."'  and dat <='".$de."')";

$result = mysqli_query($link,$query);

while($row = $result->fetch_assoc()) {
    $myArray[] = $row;
}

echo json_encode($myArray);

} 


if ($table == 'report_fbo'){       // даты

$query = "SELECT * from report_fbo WHERE (cabinet='".$cabinet."' and created_at >='".$df."'  and created_at <='".$de."')";

$result = mysqli_query($link,$query);

while($row = $result->fetch_assoc()) {
    $myArray[] = $row;
}

echo json_encode($myArray);

} 



if ($table == 'report_fbs'){       // даты

$query = "SELECT * from report_fbs WHERE (cabinet='".$cabinet."' and in_process_at >='".$df."'  and in_process_at <='".$de."')";

$result = mysqli_query($link,$query);

while($row = $result->fetch_assoc()) {
    $myArray[] = $row;
}

echo json_encode($myArray);

} 



if ($table == 'zakaz_fbo'){       // даты

$query = "SELECT * from zakaz_fbo WHERE (cabinet='".$cabinet."' and c2 >='".$df."'  and c2 <='".$de."')";

$result = mysqli_query($link,$query);

while($row = $result->fetch_assoc()) {
    $myArray[] = $row;
}

echo json_encode($myArray);

} 

if ($table == 'zakaz_fbs'){       // даты

$query = "SELECT * from zakaz_fbs WHERE (cabinet='".$cabinet."' and c2 >='".$df."'  and c2 <='".$de."')";

$result = mysqli_query($link,$query);

while($row = $result->fetch_assoc()) {
    $myArray[] = $row;
}

echo json_encode($myArray);

} 





 



 











?>