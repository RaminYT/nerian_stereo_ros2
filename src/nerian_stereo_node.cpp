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

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>

#include "rclcpp/rclcpp.hpp"
#include "std_msgs/msg/string.hpp"

#include "nerian_stereo/nerian_stereo_node.h"

using namespace std::chrono_literals;

namespace nerian_stereo {

StereoNode::StereoNode(const std::string& name)
: rclcpp::Node(
        name,
        rclcpp::NodeOptions().allow_undeclared_parameters(true)
    ), frameNum(0) {

    RCLCPP_INFO(this->get_logger(), "Creating StereoNode");
    // Declare internal parameters (device parameters are reported and added later)
    this->declare_parameter("point_cloud_intensity_channel", "mono8");
    this->declare_parameter("color_code_disparity_map",      "");
    this->declare_parameter("color_code_legend",             false);
    this->declare_parameter("top_level_frame",               "world");
    this->declare_parameter("internal_frame",                "nerian_stereo");
    this->declare_parameter("remote_port",                   "7681");
    this->declare_parameter("remote_host",                   "0.0.0.0");
    this->declare_parameter("use_tcp",                       false);
    this->declare_parameter("ros_coordinate_system",         true);
    this->declare_parameter("ros_timestamps",                true);
    this->declare_parameter("calibration_file",              "");
    this->declare_parameter("delay_execution",               0.0);
    this->declare_parameter("max_depth",                     -1);
    this->declare_parameter("q_from_calib_file",             false);

    onSetParametersCallback = this->add_on_set_parameters_callback(std::bind(&StereoNode::onSetParameters, this, std::placeholders::_1));

    // Obtain initial ROS parameter values
    std::string intensityChannel = "mono8";
    intensityChannel = this->get_parameter("point_cloud_intensity_channel").as_string();
    if(intensityChannel == "none") {
        pointCloudColorMode = NONE;
    } else if(intensityChannel == "rgb8") {
        pointCloudColorMode = RGB_COMBINED;
    } else if(intensityChannel == "rgb32f") {
        pointCloudColorMode = RGB_SEPARATE;
    } else {
        pointCloudColorMode = INTENSITY;
    }

    colorCodeDispMap = this->get_parameter("color_code_disparity_map").as_string();
    colorCodeLegend = this->get_parameter("color_code_legend").as_bool();
    frame = this->get_parameter("top_level_frame").as_string();
    internalFrame = this->get_parameter("internal_frame").as_string();
    remotePort = this->get_parameter("remote_port").as_string();
    remoteHost = this->get_parameter("remote_host").as_string();
    useTcp = this->get_parameter("use_tcp").as_bool();
    rosCoordinateSystem = this->get_parameter("ros_coordinate_system").as_bool();
    rosTimestamps  = this->get_parameter("ros_timestamps").as_bool();
    calibFile = this->get_parameter("calibration_file").as_string();
    execDelay = this->get_parameter("delay_execution").as_double();
    maxDepth = this->get_parameter("max_depth").as_int();
    useQFromCalibFile = this->get_parameter("q_from_calib_file").as_bool();
    
    lastLogTime = this->get_clock()->now();
}

void StereoNode::updateParametersFromDevice() {
    try {
        RCLCPP_INFO(this->get_logger(), "Initializing device parameters");
        deviceParameters.reset(new DeviceParameters(remoteHost.c_str()));
        auto ssParams = deviceParameters->getParameterSet();
        for (auto kv: ssParams) {
            auto& name = kv.first;
            auto& param = kv.second;
            auto description = param.getDescription();
            // Metadata
            rcl_interfaces::msg::ParameterDescriptor descriptor;
            descriptor.read_only = (param.getAccessForApi() != visiontransfer::param::Parameter::ACCESS_READWRITE);
            if (description != "") {
                descriptor.description = description;
            }
            // Value, type and specific constraints
            switch (param.getType()) {
                case visiontransfer::param::ParameterValue::TYPE_INT: {
                    if (param.hasRange()) {
                        descriptor.integer_range.resize(1);
                        auto& range = descriptor.integer_range.at(0);
                        range.from_value = param.getMin<int>();
                        range.to_value = param.getMax<int>();
                        range.step = param.hasIncrement() ? param.getIncrement<int>() : 1;
                    }
                    this->declare_parameter(name, param.getCurrent<int>(), descriptor);
                    break;
                }
                case visiontransfer::param::ParameterValue::TYPE_DOUBLE: {
                    if (param.hasRange()) {
                        descriptor.floating_point_range.resize(1);
                        auto& range = descriptor.floating_point_range.at(0);
                        range.from_value = param.getMin<double>();
                        range.to_value = param.getMax<double>();
                        range.step = param.hasIncrement() ? param.getIncrement<double>() : 1e-12;
                    }
                    this->declare_parameter(name, param.getCurrent<double>(), descriptor);
                    break;
                }
                case visiontransfer::param::ParameterValue::TYPE_BOOL: {
                    this->declare_parameter(name, param.getCurrent<bool>(), descriptor);
                    break;
                }
                case visiontransfer::param::ParameterValue::TYPE_STRING:
                case visiontransfer::param::ParameterValue::TYPE_SAFESTRING: {
                    this->declare_parameter(name, param.getCurrent<std::string>(), descriptor);
                    break;
                }
                case visiontransfer::param::ParameterValue::TYPE_TENSOR: {
                    // Not directly mappable to ROS parameter; could serialize as (read-only) string
                }
                case visiontransfer::param::ParameterValue::TYPE_COMMAND: {
                    // Expose commands as simple trigger toggles (e.g. for reboot)
                    this->declare_parameter(name, ((bool) false), descriptor);
                    break;
                }
                default:
                    throw std::runtime_error("Received parameter of unsupported type from device");
            }
        }
        availableDeviceParameters = ssParams;
        RCLCPP_INFO(this->get_logger(), "Queried device and obtained %d device parameters", (int) availableDeviceParameters.size());
    } catch(visiontransfer::ParameterException& e) {
        RCLCPP_ERROR(this->get_logger(), "ParameterException during setup of parameter service: %s", e.what());
        RCLCPP_ERROR(this->get_logger(), "Handshake with parameter server failed; device-related parameters are unavailable - please verify firmware version. Image transport is unaffected");
        return;
    } catch(visiontransfer::TransferException& e) {
        RCLCPP_ERROR(this->get_logger(), "TransferException during setup of parameter service: %s", e.what());
        RCLCPP_ERROR(this->get_logger(), "Handshake with parameter server failed; device-related parameters are unavailable - please verify firmware version. Image transport is unaffected");
        return;
    }
}

void StereoNode::init() {

    RCLCPP_INFO(this->get_logger(), "Initializing StereoNode");
    // Apply an initial delay if configured
    std::this_thread::sleep_for(std::chrono::milliseconds((int)(1000.0*execDelay)));

    // Obtain the parameters that are reported by the device and expose them
    updateParametersFromDevice();

    // Create publishers
    disparityPublisher = this->create_publisher<sensor_msgs::msg::Image>("/nerian_stereo/disparity_map", 5);
    leftImagePublisher = this->create_publisher<sensor_msgs::msg::Image>("/nerian_stereo/left_image", 5);
    rightImagePublisher = this->create_publisher<sensor_msgs::msg::Image>("/nerian_stereo/right_image", 5);

    loadCameraCalibration();

    cameraInfoPublisher = this->create_publisher<nerian_stereo::msg::StereoCameraInfo>("/nerian_stereo/stereo_camera_info", 1);
    cloudPublisher = this->create_publisher<sensor_msgs::msg::PointCloud2>("/nerian_stereo/point_cloud", 5);

    transformBroadcaster = std::make_unique<tf2_ros::TransformBroadcaster>(*this);
    currentTransform.header.stamp = this->get_clock()->now();
    currentTransform.header.frame_id = frame;
    currentTransform.child_frame_id = internalFrame;
    currentTransform.transform.translation.x = 0.0;
    currentTransform.transform.translation.y = 0.0;
    currentTransform.transform.translation.z = 0.0;
    currentTransform.transform.rotation.x = 0.0;
    currentTransform.transform.rotation.y = 0.0;
    currentTransform.transform.rotation.z = 0.0;
    currentTransform.transform.rotation.w = 1.0;

    initDataChannelService();
    //initDynamicReconfigure();
    publishTransform(); // initial transform
    prepareAsyncTransfer();
    // 2kHz timer for lower latency (stereoIteration will then block)
    timer = this->create_wall_timer(500us, std::bind(&StereoNode::stereoIteration, this));

    reactToParameterUpdates = true;
}

void StereoNode::initDataChannelService() {
    dataChannelService.reset(new DataChannelService(remoteHost.c_str()));
}

void StereoNode::prepareAsyncTransfer() {
    RCLCPP_INFO(this->get_logger(), "Connecting to %s:%s for data transfer", remoteHost.c_str(), remotePort.c_str());
    asyncTransfer.reset(new AsyncTransfer(remoteHost.c_str(), remotePort.c_str(),
        useTcp ? ImageProtocol::PROTOCOL_TCP : ImageProtocol::PROTOCOL_UDP));
}

void StereoNode::processOneImageSet() {
    // Receive image data
    ImageSet imageSet;
    if(asyncTransfer->collectReceivedImageSet(imageSet, 0.005)) {

        // Get time stamp
        rclcpp::Time stamp;
        rclcpp::Time timeNow = this->get_clock()->now();
        if(rosTimestamps) {
            stamp = timeNow;
        } else {
            int secs = 0, microsecs = 0;
            imageSet.getTimestamp(secs, microsecs);
            stamp = rclcpp::Time(secs, microsecs*1000);
        }

        // Publish image data messages for all images included in the set
        if (imageSet.hasImageType(ImageSet::IMAGE_LEFT)) {
            publishImageMsg(imageSet, imageSet.getIndexOf(ImageSet::IMAGE_LEFT), stamp, false, leftImagePublisher);
        }
        if (imageSet.hasImageType(ImageSet::IMAGE_DISPARITY)) {
            publishImageMsg(imageSet, imageSet.getIndexOf(ImageSet::IMAGE_DISPARITY), stamp, true, disparityPublisher);
        }
        if (imageSet.hasImageType(ImageSet::IMAGE_RIGHT)) {
            publishImageMsg(imageSet, imageSet.getIndexOf(ImageSet::IMAGE_RIGHT), stamp, false, rightImagePublisher);
        }

        if(cloudPublisher->get_subscription_count() > 0) {
            if(recon3d == nullptr) {
                // First initialize
                recon3d.reset(new Reconstruct3D);
            }

            publishPointCloudMsg(imageSet, stamp);
        }

        if(cameraInfoPublisher != NULL && cameraInfoPublisher->get_subscription_count() > 0) {
            publishCameraInfo(stamp, imageSet);
        }

        // Display some simple statistics
        frameNum++;
        if((int) timeNow.seconds() != (int) lastLogTime.seconds()) {
            if(lastLogTime.seconds() != 0) {
                double dt = (timeNow - lastLogTime).seconds();
                double fps = (frameNum - lastLogFrames) / dt;
                RCLCPP_INFO(this->get_logger(), "%.1f fps", fps);
            }
            lastLogFrames = frameNum;
            lastLogTime = timeNow;
        }
    }
}

void StereoNode::loadCameraCalibration() {
    if(calibFile == "" ) {
        RCLCPP_WARN(this->get_logger(), "No camera calibration file configured. Cannot publish detailed camera information!");
    } else {
        bool success = false;
        try {
            if (calibStorage.open(calibFile, cv::FileStorage::READ)) {
                success = true;
            }
        } catch(...) {
        }

        if(!success) {
            RCLCPP_WARN(this->get_logger(), "Error reading calibration file: %s\n"
                "Cannot publish detailed camera information!", calibFile.c_str());
        }
    }
}

void StereoNode::publishImageMsg(const ImageSet& imageSet, int imageIndex, rclcpp::Time stamp, bool allowColorCode,
        rclcpp::Publisher<sensor_msgs::msg::Image>::SharedPtr publisher) {

    if(publisher->get_subscription_count() <= 0) {
        return; //No subscribers
    }

    cv_bridge::CvImage cvImg;
    cvImg.header.frame_id = internalFrame;
    cvImg.header.stamp = stamp;
    //cvImg.header.seq = imageSet.getSequenceNumber(); // Actually ROS will overwrite this

    bool format12Bit = (imageSet.getPixelFormat(imageIndex) == ImageSet::FORMAT_12_BIT_MONO);
    string encoding = "";
    bool ok = true;

    if(colorCodeDispMap == "" || colorCodeDispMap == "none" || !allowColorCode || !format12Bit) {
        switch (imageSet.getPixelFormat(imageIndex)) {
            case ImageSet::FORMAT_8_BIT_RGB: {
                cv::Mat rgbImg(imageSet.getHeight(), imageSet.getWidth(),
                    CV_8UC3,
                    imageSet.getPixelData(imageIndex), imageSet.getRowStride(imageIndex));
                cvImg.image = rgbImg;
                encoding = "rgb8";
                break;
            }
            case ImageSet::FORMAT_8_BIT_MONO:
            case ImageSet::FORMAT_12_BIT_MONO: {
                cv::Mat monoImg(imageSet.getHeight(), imageSet.getWidth(),
                    format12Bit ? CV_16UC1 : CV_8UC1,
                    imageSet.getPixelData(imageIndex), imageSet.getRowStride(imageIndex));
                cvImg.image = monoImg;
                encoding = (format12Bit ? "mono16": "mono8");
                break;
            }
            default: {
                RCLCPP_WARN(this->get_logger(), "Omitting an image with unhandled pixel format");
                ok = false;
            }
        }
    } else {
        cv::Mat monoImg(imageSet.getHeight(), imageSet.getWidth(),
            format12Bit ? CV_16UC1 : CV_8UC1,
            imageSet.getPixelData(imageIndex), imageSet.getRowStride(imageIndex));

        if(colCoder == NULL) {
            int dispMin = 0, dispMax = 0;
            imageSet.getDisparityRange(dispMin, dispMax);

            colCoder.reset(new ColorCoder(
                colorCodeDispMap == "rainbow" ? ColorCoder::COLOR_RAINBOW_BGR : ColorCoder::COLOR_RED_BLUE_BGR,
                dispMin*16, dispMax*16, true, true));
            if(colorCodeLegend) {
                // Create the legend
                colDispMap = colCoder->createLegendBorder(monoImg.cols, monoImg.rows, 1.0/16.0);
            } else {
                colDispMap = cv::Mat_<cv::Vec3b>(monoImg.rows, monoImg.cols);
            }
        }

        cv::Mat_<cv::Vec3b> dispSection = colDispMap(cv::Rect(0, 0, monoImg.cols, monoImg.rows));

        colCoder->codeImage(cv::Mat_<unsigned short>(monoImg), dispSection);
        cvImg.image = colDispMap;
        encoding = "bgr8";
    }

    if (ok) {
        // Allocate and prepare
        sensor_msgs::msg::Image* msg = new sensor_msgs::msg::Image();
        cvImg.toImageMsg(*msg);
        msg->encoding = encoding;
        // Transfer and dispatch to async publish
        publisher->publish(sensor_msgs::msg::Image::UniquePtr(msg));
    }
}

void StereoNode::qMatrixToRosCoords(const float* src, float* dst) {
    dst[0] = src[8];   dst[1] = src[9];
    dst[2] = src[10];  dst[3] = src[11];

    dst[4] = -src[0];  dst[5] = -src[1];
    dst[6] = -src[2];  dst[7] = -src[3];

    dst[8] = -src[4];  dst[9] = -src[5];
    dst[10] = -src[6]; dst[11] = -src[7];

    dst[12] = src[12]; dst[13] = src[13];
    dst[14] = src[14]; dst[15] = src[15];
}

void StereoNode::publishPointCloudMsg(ImageSet& imageSet, rclcpp::Time stamp) {
    if ((!imageSet.hasImageType(ImageSet::IMAGE_DISPARITY))
        || (imageSet.getPixelFormat(ImageSet::IMAGE_DISPARITY) != ImageSet::FORMAT_12_BIT_MONO)) {
        return; // This is not a disparity map
    }

    // Set static q matrix if desired
    if(useQFromCalibFile) {
        static std::vector<float> q;
        calibStorage["Q"] >> q;
        imageSet.setQMatrix(&q[0]);
    }

    // Transform Q-matrix if desired
    float qRos[16];
    if(rosCoordinateSystem) {
        qMatrixToRosCoords(imageSet.getQMatrix(), qRos);
        imageSet.setQMatrix(qRos);
    }

    // Get 3D points
    float* pointMap = nullptr;
    try {
        pointMap = recon3d->createPointMap(imageSet, 0);
    } catch(std::exception& ex) {
        cerr << "Error creating point cloud: " << ex.what() << endl;
        return;
    }

    pointCloudMsg = new sensor_msgs::msg::PointCloud2(); // publish() will take ownership later
    initPointCloud(); // reinitialize new point cloud metadata

    // Create message object and set header
    pointCloudMsg->header.stamp = stamp;
    pointCloudMsg->header.frame_id = internalFrame;

    // Copy 3D points
    if(pointCloudMsg->data.size() != imageSet.getWidth()*imageSet.getHeight()*4*sizeof(float)) {
        // Allocate buffer
        pointCloudMsg->data.resize(imageSet.getWidth()*imageSet.getHeight()*4*sizeof(float));

        // Set basic data
        pointCloudMsg->width = imageSet.getWidth();
        pointCloudMsg->height = imageSet.getHeight();
        pointCloudMsg->is_bigendian = false;
        pointCloudMsg->point_step = 4*sizeof(float);
        pointCloudMsg->row_step = imageSet.getWidth() * pointCloudMsg->point_step;
        pointCloudMsg->is_dense = false;
    }

    if(maxDepth < 0) {
        // Just copy everything
        memcpy(&pointCloudMsg->data[0], pointMap,
            imageSet.getWidth()*imageSet.getHeight()*4*sizeof(float));
    } else {
        // Only copy points up to maximum depth
        if(rosCoordinateSystem) {
            copyPointCloudClamped<0>(pointMap, reinterpret_cast<float*>(&pointCloudMsg->data[0]),
                imageSet.getWidth()*imageSet.getHeight());
        } else {
            copyPointCloudClamped<2>(pointMap, reinterpret_cast<float*>(&pointCloudMsg->data[0]),
                imageSet.getWidth()*imageSet.getHeight());
        }
    }

    if (imageSet.hasImageType(ImageSet::IMAGE_LEFT)) {
        // Copy intensity values as well (if we received any image data)
        switch(pointCloudColorMode) {
            case INTENSITY:
                copyPointCloudIntensity<INTENSITY>(imageSet);
                break;
            case RGB_COMBINED:
                copyPointCloudIntensity<RGB_COMBINED>(imageSet);
                break;
            case RGB_SEPARATE:
                copyPointCloudIntensity<RGB_SEPARATE>(imageSet);
                break;
            case NONE:
                break;
        }
    }

    cloudPublisher->publish(sensor_msgs::msg::PointCloud2::UniquePtr(pointCloudMsg));
}

template <StereoNode::PointCloudColorMode colorMode> void StereoNode::copyPointCloudIntensity(ImageSet& imageSet) {
    // Get pointers to the beginning and end of the point cloud
    unsigned char* cloudStart = &pointCloudMsg->data[0];
    unsigned char* cloudEnd = &pointCloudMsg->data[0]
        + imageSet.getWidth()*imageSet.getHeight()*4*sizeof(float);

    if(imageSet.getPixelFormat(ImageSet::IMAGE_LEFT) == ImageSet::FORMAT_8_BIT_MONO) {
        // Get pointer to the current pixel and end of current row
        unsigned char* imagePtr = imageSet.getPixelData(ImageSet::IMAGE_LEFT);
        unsigned char* rowEndPtr = imagePtr + imageSet.getWidth();
        int rowIncrement = imageSet.getRowStride(ImageSet::IMAGE_LEFT) - imageSet.getWidth();

        for(unsigned char* cloudPtr = cloudStart + 3*sizeof(float);
                cloudPtr < cloudEnd; cloudPtr+= 4*sizeof(float)) {
            if(colorMode == RGB_SEPARATE) {// RGB as float
                *reinterpret_cast<float*>(cloudPtr) = static_cast<float>(*imagePtr) / 255.0F;
            } else if(colorMode == RGB_COMBINED) {// RGB as integer
                const unsigned char intensity = *imagePtr;
                *reinterpret_cast<unsigned int*>(cloudPtr) = (intensity << 16) | (intensity << 8) | intensity;
            } else {
                *cloudPtr = *imagePtr;
            }

            imagePtr++;
            if(imagePtr == rowEndPtr) {
                // Progress to next row
                imagePtr += rowIncrement;
                rowEndPtr = imagePtr + imageSet.getWidth();
            }
        }
    } else if(imageSet.getPixelFormat(ImageSet::IMAGE_LEFT) == ImageSet::FORMAT_12_BIT_MONO) {
        // Get pointer to the current pixel and end of current row
        unsigned short* imagePtr = reinterpret_cast<unsigned short*>(imageSet.getPixelData(ImageSet::IMAGE_LEFT));
        unsigned short* rowEndPtr = imagePtr + imageSet.getWidth();
        int rowIncrement = imageSet.getRowStride(ImageSet::IMAGE_LEFT) - 2*imageSet.getWidth();

        for(unsigned char* cloudPtr = cloudStart + 3*sizeof(float);
                cloudPtr < cloudEnd; cloudPtr+= 4*sizeof(float)) {

            if(colorMode == RGB_SEPARATE) {// RGB as float
                *reinterpret_cast<float*>(cloudPtr) = static_cast<float>(*imagePtr) / 4095.0F;
            } else if(colorMode == RGB_COMBINED) {// RGB as integer
                const unsigned char intensity = *imagePtr/16;
                *reinterpret_cast<unsigned int*>(cloudPtr) = (intensity << 16) | (intensity << 8) | intensity;
            } else {
                *cloudPtr = *imagePtr/16;
            }

            imagePtr++;
            if(imagePtr == rowEndPtr) {
                // Progress to next row
                imagePtr += rowIncrement;
                rowEndPtr = imagePtr + imageSet.getWidth();
            }
        }
    } else if(imageSet.getPixelFormat(ImageSet::IMAGE_LEFT) == ImageSet::FORMAT_8_BIT_RGB) {
        // Get pointer to the current pixel and end of current row
        unsigned char* imagePtr = imageSet.getPixelData(ImageSet::IMAGE_LEFT);
        unsigned char* rowEndPtr = imagePtr + 3*imageSet.getWidth();
        int rowIncrement = imageSet.getRowStride(ImageSet::IMAGE_LEFT) - 3*imageSet.getWidth();

        static bool warned = false;
        if(colorMode == RGB_SEPARATE && !warned) {
            warned = true;
            RCLCPP_WARN(this->get_logger(), "RGBF32 is not supported for color images. Please use RGB8!");
        }

        for(unsigned char* cloudPtr = cloudStart + 3*sizeof(float);
                cloudPtr < cloudEnd; cloudPtr+= 4*sizeof(float)) {
            if(colorMode == RGB_SEPARATE) {// RGB as float
                *reinterpret_cast<float*>(cloudPtr) = static_cast<float>(imagePtr[2]) / 255.0F;
            } else if(colorMode == RGB_COMBINED) {// RGB as integer
                *reinterpret_cast<unsigned int*>(cloudPtr) = (imagePtr[0] << 16) | (imagePtr[1] << 8) | imagePtr[2];
            } else {
                *cloudPtr = (imagePtr[0] + imagePtr[1]*2 + imagePtr[2])/4;
            }

            imagePtr+=3;
            if(imagePtr == rowEndPtr) {
                // Progress to next row
                imagePtr += rowIncrement;
                rowEndPtr = imagePtr + imageSet.getWidth();
            }
        }
    } else {
        throw std::runtime_error("Invalid pixel format!");
    }
}

template <int coord> void StereoNode::copyPointCloudClamped(float* src, float* dst, int size) {
    // Only copy points that are below the minimum depth
    float* endPtr = src + 4*size;
    for(float *srcPtr = src, *dstPtr = dst; srcPtr < endPtr; srcPtr+=4, dstPtr+=4) {
        if(srcPtr[coord] > maxDepth) {
            dstPtr[0] = std::numeric_limits<float>::quiet_NaN();
            dstPtr[1] = std::numeric_limits<float>::quiet_NaN();
            dstPtr[2] = std::numeric_limits<float>::quiet_NaN();
        } else {
            dstPtr[0] = srcPtr[0];
            dstPtr[1] = srcPtr[1];
            dstPtr[2] = srcPtr[2];
        }
    }
}

void StereoNode::initPointCloud() {

    // Set channel information.
    sensor_msgs::msg::PointField fieldX;
    fieldX.name ="x";
    fieldX.offset = 0;
    fieldX.datatype = sensor_msgs::msg::PointField::FLOAT32;
    fieldX.count = 1;
    pointCloudMsg->fields.push_back(fieldX);

    sensor_msgs::msg::PointField fieldY;
    fieldY.name ="y";
    fieldY.offset = sizeof(float);
    fieldY.datatype = sensor_msgs::msg::PointField::FLOAT32;
    fieldY.count = 1;
    pointCloudMsg->fields.push_back(fieldY);

    sensor_msgs::msg::PointField fieldZ;
    fieldZ.name ="z";
    fieldZ.offset = 2*sizeof(float);
    fieldZ.datatype = sensor_msgs::msg::PointField::FLOAT32;
    fieldZ.count = 1;
    pointCloudMsg->fields.push_back(fieldZ);

    if(pointCloudColorMode == INTENSITY) {
        sensor_msgs::msg::PointField fieldI;
        fieldI.name ="intensity";
        fieldI.offset = 3*sizeof(float);
        fieldI.datatype = sensor_msgs::msg::PointField::UINT8;
        fieldI.count = 1;
        pointCloudMsg->fields.push_back(fieldI);
    }
    else if(pointCloudColorMode == RGB_SEPARATE) {
        sensor_msgs::msg::PointField fieldRed;
        fieldRed.name ="r";
        fieldRed.offset = 3*sizeof(float);
        fieldRed.datatype = sensor_msgs::msg::PointField::FLOAT32;
        fieldRed.count = 1;
        pointCloudMsg->fields.push_back(fieldRed);

        sensor_msgs::msg::PointField fieldGreen;
        fieldGreen.name ="g";
        fieldGreen.offset = 3*sizeof(float);
        fieldGreen.datatype = sensor_msgs::msg::PointField::FLOAT32;
        fieldGreen.count = 1;
        pointCloudMsg->fields.push_back(fieldGreen);

        sensor_msgs::msg::PointField fieldBlue;
        fieldBlue.name ="b";
        fieldBlue.offset = 3*sizeof(float);
        fieldBlue.datatype = sensor_msgs::msg::PointField::FLOAT32;
        fieldBlue.count = 1;
        pointCloudMsg->fields.push_back(fieldBlue);
    } else if(pointCloudColorMode == RGB_COMBINED) {
        sensor_msgs::msg::PointField fieldRGB;
        fieldRGB.name ="rgb";
        fieldRGB.offset = 3*sizeof(float);
        fieldRGB.datatype = sensor_msgs::msg::PointField::UINT32;
        fieldRGB.count = 1;
        pointCloudMsg->fields.push_back(fieldRGB);
    }
}

void StereoNode::publishCameraInfo(rclcpp::Time stamp, const ImageSet& imageSet) {
    if(camInfoMsg == NULL) {
        // Initialize the camera info structure
        camInfoMsg.reset(new nerian_stereo::msg::StereoCameraInfo);

        camInfoMsg->header.frame_id = internalFrame;

        if(calibFile != "") {
            std::vector<int> sizeVec;
            calibStorage["size"] >> sizeVec;
            if(sizeVec.size() != 2) {
                std::runtime_error("Calibration file format error!");
            }

            camInfoMsg->left_info.header = camInfoMsg->header;
            camInfoMsg->left_info.width = sizeVec[0];
            camInfoMsg->left_info.height = sizeVec[1];
            camInfoMsg->left_info.distortion_model = "plumb_bob";
            calibStorage["D1"] >> camInfoMsg->left_info.d;
            readCalibrationArray("M1", camInfoMsg->left_info.k);
            readCalibrationArray("R1", camInfoMsg->left_info.r);
            readCalibrationArray("P1", camInfoMsg->left_info.p);
            camInfoMsg->left_info.binning_x = 1;
            camInfoMsg->left_info.binning_y = 1;
            camInfoMsg->left_info.roi.do_rectify = false;
            camInfoMsg->left_info.roi.height = 0;
            camInfoMsg->left_info.roi.width = 0;
            camInfoMsg->left_info.roi.x_offset = 0;
            camInfoMsg->left_info.roi.y_offset = 0;

            camInfoMsg->right_info.header = camInfoMsg->header;
            camInfoMsg->right_info.width = sizeVec[0];
            camInfoMsg->right_info.height = sizeVec[1];
            camInfoMsg->right_info.distortion_model = "plumb_bob";
            calibStorage["D2"] >> camInfoMsg->right_info.d;
            readCalibrationArray("M2", camInfoMsg->right_info.k);
            readCalibrationArray("R2", camInfoMsg->right_info.r);
            readCalibrationArray("P2", camInfoMsg->right_info.p);
            camInfoMsg->right_info.binning_x = 1;
            camInfoMsg->right_info.binning_y = 1;
            camInfoMsg->right_info.roi.do_rectify = false;
            camInfoMsg->right_info.roi.height = 0;
            camInfoMsg->right_info.roi.width = 0;
            camInfoMsg->right_info.roi.x_offset = 0;
            camInfoMsg->right_info.roi.y_offset = 0;

            readCalibrationArray("Q", camInfoMsg->q);
            readCalibrationArray("T", camInfoMsg->t_left_right);
            readCalibrationArray("R", camInfoMsg->r_left_right);
        }
    }

    double dt = (stamp - lastCamInfoPublish).seconds();
    if(dt > 1.0) {
        // Rather use the Q-matrix that we received over the network if it is valid
        const float* qMatrix = imageSet.getQMatrix();
        if(qMatrix[0] != 0.0) {
            for(int i=0; i<16; i++) {
                camInfoMsg->q[i] = static_cast<double>(qMatrix[i]);
            }
        }

        // Publish once per second
        camInfoMsg->header.stamp = stamp;
        camInfoMsg->left_info.header.stamp = stamp;
        camInfoMsg->right_info.header.stamp = stamp;
        cameraInfoPublisher->publish(*camInfoMsg);

        lastCamInfoPublish = stamp;
    }
}

template<class T> void StereoNode::readCalibrationArray(const char* key, T& dest) {
    std::vector<double> doubleVec;
    calibStorage[key] >> doubleVec;

    if(doubleVec.size() != dest.size()) {
        std::runtime_error("Calibration file format error!");
    }

    std::copy(doubleVec.begin(), doubleVec.end(), dest.begin());
}

void StereoNode::processDataChannels() {
    auto now = this->get_clock()->now();
    if ((now - currentTransform.header.stamp).seconds() < 0.01) {
        // Limit to 100 Hz transform update frequency
        return;
    }
    if (dataChannelService->imuAvailable()) {
        // Obtain and publish the most recent orientation
        TimestampedQuaternion tsq = dataChannelService->imuGetRotationQuaternion();
        currentTransform.header.stamp = now;
        if(rosCoordinateSystem) {
            currentTransform.transform.rotation.x = tsq.x();
            currentTransform.transform.rotation.y = -tsq.z();
            currentTransform.transform.rotation.z = tsq.y();
        } else {
            currentTransform.transform.rotation.x = tsq.x();
            currentTransform.transform.rotation.y = tsq.y();
            currentTransform.transform.rotation.z = tsq.z();
        }
        currentTransform.transform.rotation.w = tsq.w();

        /*
        // DEBUG: Quaternion->Euler + debug output
        double roll, pitch, yaw;
        tf2::Quaternion q(tsq.x(), rosCoordinateSystem?(-tsq.z()):tsq.y(), rosCoordinateSystem?tsq.y():tsq.z(), tsq.w());
        tf2::Matrix3x3 m(q);
        m.getRPY(roll, pitch, yaw);
        std::cout << "Orientation:" << std::setprecision(2) << std::fixed << " Roll " << (180.0*roll/M_PI) << " Pitch " << (180.0*pitch/M_PI) << " Yaw " << (180.0*yaw/M_PI) << std::endl;
        */

        publishTransform();
    } else {
        // We must periodically republish due to ROS interval constraints
        /*
        // DEBUG: Impart a (fake) periodic horizontal swaying motion
        static double DEBUG_t = 0.0;
        DEBUG_t += 0.1;
        tf2::Quaternion q;
        q.setRPY(0, 0, 0.3*sin(DEBUG_t));
        currentTransform.header.stamp = this->get_clock()->now();
        currentTransform.transform.rotation.x = q.x();
        currentTransform.transform.rotation.y = q.y();
        currentTransform.transform.rotation.z = q.z();
        currentTransform.transform.rotation.w = q.w();
        */
        currentTransform.header.stamp = now;
        publishTransform();
    }
}

void StereoNode::publishTransform() {
    transformBroadcaster->sendTransform(currentTransform);
}

void StereoNode::stereoIteration() {
    processOneImageSet();
    processDataChannels();
}

rcl_interfaces::msg::SetParametersResult StereoNode::onSetParameters(std::vector<rclcpp::Parameter> parameters) {
    auto result = rcl_interfaces::msg::SetParametersResult();
    result.successful = true;
    if (!reactToParameterUpdates) return result;
    for (auto parameter : parameters) {
        rclcpp::ParameterType parameter_type = parameter.get_type();
        std::string name = parameter.get_name();
        auto old_type = this->get_parameter(name).get_type();
        if (parameter_type == old_type) {
            if (acceptedInternalParameters.count(name)) {
                if (name == "max_depth") {
                    maxDepth = parameter.as_int();
                    RCLCPP_INFO(this->get_logger(), "Set internal parameter '%s' to %s", name.c_str(), parameter.value_to_string().c_str());
                    result.successful &= true;
                } else if (name == "top_level_frame") {
                    // TODO Frame name validation to only accept what ROS2 expects
                    frame = parameter.as_string();
                    currentTransform.header.frame_id = frame;
                } else if (name == "internal_frame") {
                    // TODO Frame name validation to only accept what ROS2 expects
                    internalFrame = parameter.as_string();
                    currentTransform.child_frame_id = internalFrame;
                } else {
                    RCLCPP_ERROR(this->get_logger(), "(Bug?) Unhandled internal parameter, cannot set '%s' to %s", name.c_str(), parameter.value_to_string().c_str());
                    result.successful = false;
                }
            } else if (availableDeviceParameters.count(name)) { // TODO modify this to use internal ParameterSet
                bool ok = true;
                try {
                    switch (parameter_type) {
                        case rclcpp::ParameterType::PARAMETER_INTEGER: {
                            deviceParameters->setParameter(name, (int) parameter.as_int());
                            ok = true;
                            break;
                        }
                        case rclcpp::ParameterType::PARAMETER_DOUBLE: {
                            deviceParameters->setParameter(name, (double) parameter.as_double());
                            ok = true;
                            break;
                        }
                        case rclcpp::ParameterType::PARAMETER_BOOL: {
                            deviceParameters->setParameter(name, (bool) parameter.as_bool());
                            ok = true;
                            break;
                        }
                        case rclcpp::ParameterType::PARAMETER_STRING: {
                            std::string val = parameter.as_string();
                            deviceParameters->setParameter(name, val);
                            ok = true;
                            break;
                        }
                        default: {
                            RCLCPP_ERROR(this->get_logger(), "Cannot handle requested parameter type for '%s'.", name.c_str());
                            ok = false;
                        }
                    }
                } catch(visiontransfer::ParameterException& e) {
                    RCLCPP_ERROR(this->get_logger(), "ParameterException for parameter set operation for parameter %s: %s", name.c_str(), e.what());
                    result.reason = e.what();
                    ok = false;
                } catch(visiontransfer::TransferException& e) {
                    RCLCPP_ERROR(this->get_logger(), "TransferException for parameter set operation for parameter %s: %s", name.c_str(), e.what());
                    result.reason = e.what();
                    ok = false;
                }
                if (ok) {
                    RCLCPP_INFO(this->get_logger(), "Set device parameter '%s' to %s", name.c_str(), parameter.value_to_string().c_str());
                } else {
                    result.successful = false;
                }
            } else if (rejectedInternalParameters.count(name)) {
                RCLCPP_ERROR(this->get_logger(), "Internal parameter '%s' cannot be modified at runtime.", name.c_str());
                result.successful = false;
            } else {
                RCLCPP_ERROR(this->get_logger(), "Will not set uninitialized parameter '%s'.", name.c_str());
                result.successful = false;
            }
        } else if (parameter_type == rclcpp::ParameterType::PARAMETER_NOT_SET) {
            RCLCPP_ERROR(this->get_logger(), "Refusing to delete parameter '%s'.", name.c_str());
            result.successful = false;
        } else {
            if (old_type ==  rclcpp::ParameterType::PARAMETER_NOT_SET) {
                RCLCPP_ERROR(this->get_logger(), "Refusing to add uninitialized parameter '%s'.", name.c_str());
            } else {
                RCLCPP_ERROR(this->get_logger(), "Mismatched type for parameter '%s'.", name.c_str());
            }
            result.successful = false;
        }
    }
    return result;
}

} // namespace

int main(int argc, char * argv[])
{
    rclcpp::init(argc, argv);
    auto node = std::make_shared<nerian_stereo::StereoNode>();
    node->init();
    rclcpp::spin(node);
    rclcpp::shutdown();
    return 0;
}

