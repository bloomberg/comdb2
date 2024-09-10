error() {
	err "Failed at line $1"
	exit 1
}

function failif {
	if [[ $1 -ne 0 ]]; then
		exit 1
	fi
}

