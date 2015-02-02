begin {
	test_account = "111111111111"
	$set_metadata_by_account(test_account, "$$test1", 1)
}
body {
	$log($account_id + " metadata " + $$test1)
}
end {
	$log("test_done")
}