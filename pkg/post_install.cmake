foreach(dir etc lib log var var/lib var/log)
  file(MAKE_DIRECTORY $ENV{DESTDIR}/${CMAKE_INSTALL_PREFIX}/${dir}/cdb2)
endforeach()
file(MAKE_DIRECTORY $ENV{DESTDIR}/${CMAKE_INSTALL_PREFIX}/etc/cdb2/config/comdb2.d)
set(ICTHX
"
Thank you for installing Comdb2.

Just a few more steps left.  Installing Comdb2 from a package takes care of
these for you, but installing from source requires a bit of manual work.

* REQURIED: running pmux
  'pmux' is a service Comdb2 uses to resolve database ports.  If you're running
  Comdb2 as an experiment, you can run it now as 'pmux -l'.  If you'd like it
  to run persistently, we provided a service file in

  ${CMAKE_INSTALL_PREFIX}/lib/systemd/system/pmux.service

  You'll need to edit the User= line to reflect the user that you'll want to
  use for running databases.  pmux can be installed to start automatically
  with:

  sudo cp ${CMAKE_INSTALL_PREFIX}/lib/systemd/system/pmux.service /lib/systemd/system/pmux.service
  sudo systemctl daemon-reload
  sudo systemctl enable pmux
  sudo systemctl start pmux
"
#[[
* SUGGESTED: running supervisord
  Supervisord is a process manager than can manage running database instances.
  Using it is not required, but will allow you to use comdb2admin for easy
  database maintenance.  Supervisord will run without elevated priviledges,
  and the install will not interfere with other installs of supervisord on
  your machine.  A sample config file is provided in

  ${CMAKE_INSTALL_PREFIX}/lib/systemd/system/supervisor_cdb2.service

  Please edit the 'User=' line to set the user that'll be running databases.
  You can configure it to start automatically with:

  sudo cp ${CMAKE_INSTALL_PREFIX}/lib/systemd/system/supervisor_cdb2.service /lib/systemd/system/supervisor_cdb2.service
  sudo systemctl daemon-reload
  sudo systemctl enable supervisor_cdb2
  sudo systemctl start supervisor_cdb2
]]
)
#message(${ICTHX})
