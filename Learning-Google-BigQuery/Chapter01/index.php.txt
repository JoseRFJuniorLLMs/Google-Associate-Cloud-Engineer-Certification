<?php
$dsn = "mysql:unix_socket=/cloudsql/my-first-project-170319:us-central1:trainingdbserver;dbname=EmployeeMgmt";
$username = "root";
$password = "password";

// Create connection
$db = new PDO($dsn, $username, $password);

$results = $db->query('SELECT * from EmployeeDetails');

foreach ($results as $row) {
        echo $row['EmployeeID'] . "\t";
        echo $row['FirstName'] . "\t";
        echo $row['LastName'] . "<br>";
    }

?>
