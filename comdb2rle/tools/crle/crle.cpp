#include <comdb2rle.c>
#include <iostream>
#include <string>
#include <vector>
#include <iterator>
#include <iomanip>
#include <fstream>
#include <tuple>
#include <utility>
#include <memory>

using namespace std;

int main(int argc, char *argv[])
{
	string verbose{"-v"};
	string extn{".crle"};

	typedef istreambuf_iterator<char> ii_t;
	typedef pair<ifstream *, ii_t> is_t;

	typedef ostreambuf_iterator<char> oi_t;
	typedef pair<ofstream *, oi_t> os_t;

	vector<tuple<const char *, is_t, os_t>> v;

	int i = 1;
	if (argv[1] && argv[1] == verbose) {
		doprint = 1;
		i = 2;
	}

	if (i == argc) {
		v.emplace_back(make_tuple("stdin", 
					  make_pair(nullptr, ii_t{cin}),
					  make_pair(nullptr, oi_t{cout})));
	} else do {
		ifstream *ifs = new ifstream {argv[i], ios::binary};
		ofstream *ofs = new ofstream {argv[i] + extn, ios::binary};
		v.emplace_back(make_tuple(argv[i],
					  make_pair(ifs, ii_t{*ifs}),
					  make_pair(ofs, oi_t{*ofs})));
	} while (argv[++i]);

	ii_t eos{};
	for (auto it : v) {
		string f {get<0>(it)};
		is_t i = get<1>(it);
		os_t o = get<2>(it);
		unique_ptr<ifstream> ifs {i.first};
		unique_ptr<ofstream> ofs {o.first};
		vector<uint8_t> ip {i.second, eos};
		vector<uint8_t> op(ip.size() * 2);
		Comdb2RLE rle {.in = ip.data(), .insz = ip.size(),
			       .out = op.data(), .outsz = op.capacity()};
		if (compressComdb2RLE(&rle) != 0) {
			cerr << "Failed " << f << endl;
			continue;
		}
		copy(&op[0], &op[rle.outsz], o.second);
		if (doprint) cerr << "Success " << f << endl;
	}
}
