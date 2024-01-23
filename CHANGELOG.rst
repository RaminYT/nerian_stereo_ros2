^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Changelog for package nerian_stereo
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1.2.1 (2024-01-23)
------------------
* Updated vision transfer to version 10.6.0
* Migration to git submodule in lieu of src release archive
* Support for setting ROS-overridden device parameters after connection
* Contributors: Konstantin Schauwecker, Ramin Yaghoubzadeh Torky

1.2.0 (2023-01-11)
------------------
* Fixed parameter issue. Enabled NERIAN_ROS_DEBUG="params" for extra logging.
* Added log messages about actively served topics (based on run-time conf)
* Support for third camera (Ruby), selected for point cloud if active
* Contributors: Dr. Konstantin Schauwecker, Ramin Yaghoubzadeh Torky

1.1.0 (2022-06-30)
------------------
* Updated vision software relase to 10.0.0
* Fixed potential problem with time sources in logging code
* Support for Humble
* Initiate image transfer despite any failure to connect to parameter service.
  (Device-related parameters are unavailable then - a verbose error is logged.)
* Contributors: Konstantin Schauwecker, Ramin Yaghoubzadeh Torky

1.0.3 (2022-03-01)
------------------
* Updated vision software release
* fixed point cloud color channel issue with large RGB images
* Contributors: Konstantin Schauwecker

1.0.0 (2021-04-14)
------------------
* Initial release for ROS2