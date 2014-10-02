begin {
	complains = $get_complaints("aws china<penyu@amazon.com>")
	$log("complains: " + complains)
	bounces = $get_bounces("aws china<penyu@amazon.com>")
	$log("bounces: " + bounces)
}
body {
	for complain in complains {
		if ($1.email == complain)
			$$complain = 1
	}
	for bounce in bounces {
		if ($1.email == bounce)
			$$bounce = 1
	}
}
end {
}