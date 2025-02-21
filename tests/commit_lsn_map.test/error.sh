function failif {
	if [[ $1 -ne 0 ]]; then
		exit 1
	fi
}

