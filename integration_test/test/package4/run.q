begin {
	filename = "test.txt"
	$write_to_file(filename, "first line", "create")
	$write_to_file(filename, "second line", "append")
	$write_to_file(filename, "third line", "append")
}
body {
}
end {
	array1 = $get_file_to_array(filename)
	for line in array1 {
		$log(line)
	}
}