begin {
	a = 0
	b = 0.00
	if (a == b)
		$log("test1 equal")
	else
		$log("test1 not equal")
	a = 0
	b = 0.01
	if (a == b)
		$log("test2 equal")
	else
		$log("test2 not equal")
	a = "0"
	b = 0.00
	if (a == b)
		$log("test3 equal")
	else
		$log("test3 not equal")
	a = "0"
	b = 0.01
	if (a == b)
		$log("test4 equal")
	else
		$log("test4 not equal")
	a = "0"
	b = "0.00"
	if (a == b)
		$log("test5 equal")
	else
		$log("test5 not equal")
	a = "0"
	b = "0.01"
	if (a == b)
		$log("test6 equal")
	else
		$log("test6 not equal")
	
}
body {
}
end {
}
