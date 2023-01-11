/*******************************************************************************
 * Copyright (c) 2021 Nerian Vision GmbH
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *******************************************************************************/


#ifndef __NERIAN_STEREO_NODE_H__
#define __NERIAN_STEREO_NODE_H__

#include <iostream>
#include <iomanip>
#include <memory>

#include <visiontransfer/asynctransfer.h>
#include <visiontransfer/reconstruct3d.h>
#include <visiontransfer/deviceparameters.h>
#include <visiontransfer/exceptions.h>
#include <visiontransfer/datachannelservice.h>

#include <rclcpp/rclcpp.hpp>
#include <tf2_msgs/msg/tf_message.hpp>

#include <sensor_msgs/msg/point_cloud2.hpp>
#include <sensor_msgs/msg/image.hpp>
#include <tf2/LinearMath/Quaternion.h>
#include <tf2/LinearMath/Matrix3x3.h>
#include <tf2_ros/transform_broadcaster.h>
#include <geometry_msgs/msg/transform_stamped.hpp>

#include <cv_bridge/cv_bridge.h>
#include <opencv2/opencv.hpp>

#include <colorcoder.h>

//#include <nerian_stereo/NerianStereoConfig.h>
#include <nerian_stereo/msg/stereo_camera_info.hpp>


using namespace std;
using namespace visiontransfer;

/**
 * \brief A driver node that receives data from Nerian stereo devices and forwards
 * it to ROS.
 *
 * SceneScan and Scarlet by Nerian Vision GmbH are hardware systems for
 * real-time stereo vision. They transmit a computed disparity map (an
 * inverse depth map) through gigabit ethernet, which is then received by
 * this node. The node converts the received data into ROS messages, which
 * contain the following data:
 *
 * - Point cloud of reconstructed 3D locations
 * - Disparity map with optional color coding
 * - Rectified left camera image
 *
 * In addition, camera calibration information is also published. For
 * configuration parameters, please see the provided example launch file.
 * For more information about Nerian's stereo systems, please visit
 * http://nerian.com/products/scenescan-stereo-vision/
 */

namespace nerian_stereo {

class StereoNode: public rclcpp::Node {
public:
    StereoNode(const std::string& name="nerian_stereo");

    ~StereoNode() {
    }

    /**
     * \brief Performs general initializations
     */
    void init();

    /*
     * \brief Initialize the data channel service (for receiving e.g. internal IMU data)
     */
    void initDataChannelService();

    /**
     * \brief Connects to the image service to request the stream of image sets
     */
    void prepareAsyncTransfer();

    /*
     * \brief Collect and process a single image set (or return after timeout if none are available)
     */
    void processOneImageSet();

    /*
     * \brief Queries the the supplemental data channels (IMU ...) for new data and updates ROS accordingly
     */
    void processDataChannels();

    /*
     * \brief Publishes an update for the ROS transform
     */
    void publishTransform();

    /*
     * \brief Enable additional ROS2 log messages related to parameter setup.
     */
    void enableDebugMessagesParameters(bool enable=true) {
        debugMessagesParameters = enable;
    }

private:
    enum PointCloudColorMode {
        RGB_SEPARATE,
        RGB_COMBINED,
        INTENSITY,
        NONE
    };

    rclcpp::Publisher<sensor_msgs::msg::PointCloud2>::SharedPtr cloudPublisher;
    rclcpp::Publisher<sensor_msgs::msg::Image>::SharedPtr disparityPublisher;
    rclcpp::Publisher<sensor_msgs::msg::Image>::SharedPtr leftImagePublisher;
    rclcpp::Publisher<sensor_msgs::msg::Image>::SharedPtr rightImagePublisher;
    rclcpp::Publisher<sensor_msgs::msg::Image>::SharedPtr thirdImagePublisher;
    rclcpp::Publisher<nerian_stereo::msg::StereoCameraInfo>::SharedPtr cameraInfoPublisher;

    std::unique_ptr<tf2_ros::TransformBroadcaster> transformBroadcaster;

    // Connection to parameter server on device
    std::unique_ptr<DeviceParameters> deviceParameters;
    bool reactToParameterUpdates = false;
    visiontransfer::param::ParameterSet availableDeviceParameters;
    const std::set<std::string> acceptedInternalParameters = {
        "top_level_frame",
        "internal_frame",
        "max_depth",
    };
    const std::set<std::string> rejectedInternalParameters = {
        "color_code_disparity_map",
        "color_code_legend",
        "remote_port",
        "remote_host",
        "use_tcp",
        "ros_coordinate_system",
        "ros_timestamps",
        "calibration_file",
        "delay_execution",
        "q_from_calib_file",
    };

    // Handler for external parameter change attempts
    rclcpp::Node::OnSetParametersCallbackHandle::SharedPtr onSetParametersCallback;

    // Parameters
    bool useTcp;
    std::string colorCodeDispMap;
    bool colorCodeLegend;
    bool rosCoordinateSystem;
    bool rosTimestamps;
    std::string remotePort;
    std::string frame; // outer frame (e.g. world)
    std::string internalFrame; // our private frame / Transform we publish
    std::string remoteHost;
    std::string calibFile;
    double execDelay;
    double maxDepth;
    bool useQFromCalibFile;
    PointCloudColorMode pointCloudColorMode;

    // Other members
    int frameNum;
    std::unique_ptr<Reconstruct3D> recon3d;
    std::unique_ptr<ColorCoder> colCoder;
    cv::Mat_<cv::Vec3b> colDispMap;
    sensor_msgs::msg::PointCloud2* pointCloudMsg;
    cv::FileStorage calibStorage;
    nerian_stereo::msg::StereoCameraInfo::UniquePtr camInfoMsg;
    rclcpp::Time lastCamInfoPublish;

    // Active channels in the previous ImageSet
    bool hadLeft, hadRight, hadColor, hadDisparity;

    std::unique_ptr<AsyncTransfer> asyncTransfer;
    rclcpp::Time lastLogTime;
    int lastLogFrames = 0;

    rclcpp::TimerBase::SharedPtr timer;

    // DataChannelService connection, to obtain IMU data
    std::unique_ptr<DataChannelService> dataChannelService;
    // Our transform, updated with polled IMU data (if available)
    geometry_msgs::msg::TransformStamped currentTransform;

    // Extra debug messages
    bool debugMessagesParameters;

    /**
     * \brief Loads a camera calibration file if configured
     */
    void loadCameraCalibration();

    /**
     * \brief Publishes the disparity map as 16-bit grayscale image or color coded
     * RGB image
     */
    void publishImageMsg(const ImageSet& imageSet, int imageIndex, rclcpp::Time stamp, bool allowColorCode,
            rclcpp::Publisher<sensor_msgs::msg::Image>::SharedPtr publisher);

    /**
     * \brief Transform Q matrix to match the ROS coordinate system:
     * Swap y/z axis, then swap x/y axis, then invert y and z axis.
     */
    void qMatrixToRosCoords(const float* src, float* dst);

    /**
     * \brief Reconstructs the 3D locations form the disparity map and publishes them
     * as point cloud.
     */
    void publishPointCloudMsg(ImageSet& imageSet, rclcpp::Time stamp);

    /**
     * \brief Copies the intensity or RGB data to the point cloud
     */
    template <PointCloudColorMode colorMode> void copyPointCloudIntensity(ImageSet& imageSet);

    /**
     * \brief Copies all points in a point cloud that have a depth smaller
     * than maxDepth. Other points are set to NaN.
     */
    template <int coord> void copyPointCloudClamped(float* src, float* dst, int size);

    /**
     * \brief Performs all neccessary initializations for point cloud+
     * publishing
     */
    void initPointCloud();

    /**
     * \brief Publishes the camera info once per second
     */
    void publishCameraInfo(rclcpp::Time stamp, const ImageSet& imageSet);

    /**
     * \brief Reads a vector from the calibration file
     */
    template<class T> void readCalibrationArray(const char* key, T& dest);

    /**
     * \brief Timer callback that polls the image and data channel receivers
     */
    void stereoIteration();

    /**
     * \brief Parameter change callback for ROS2 to range check and try to relay to device (if applicable)
     */
    rcl_interfaces::msg::SetParametersResult onSetParameters(std::vector<rclcpp::Parameter> parameters);

    /**
     * \brief Obtain the parameter enumeration from the device and relay them to ROS2 parameters
     */
    void updateParametersFromDevice();
};


} // namespace

#endif

