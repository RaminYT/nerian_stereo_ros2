#define BOOST_BIND_NO_PLACEHOLDERS
#include <chrono>
#include <functional>
#include <memory>
#include <string>

#include "rclcpp/rclcpp.hpp"
#include "nerian_stereo/nerian_stereo_node.h"
#include "sensor_msgs/msg/camera_info.h"

using namespace std::chrono_literals;
using std::placeholders::_1;

class CameraInfoSplitter : public rclcpp::Node
{
    public:
        CameraInfoSplitter()
        : Node("camera_info_splitter")
        {
            nerian_camera_info_ = this->create_subscription<nerian_stereo::msg::StereoCameraInfo>("/nerian_stereo/stereo_camera_info", 10, std::bind(&CameraInfoSplitter::nerian_info_callback, this, _1));
            camera_left_info_ = this->create_publisher<sensor_msgs::msg::CameraInfo>("/nerian_stereo/left_camera_info", 10);
            camera_right_info_ = this->create_publisher<sensor_msgs::msg::CameraInfo>("/nerian_stereo/right_camera_info", 10);
        }
    
    private:
        //Declaring the subscriber variable
        rclcpp::Subscription<nerian_stereo::msg::StereoCameraInfo>::SharedPtr nerian_camera_info_;

        //Declare the publishers for the camera left and right
        rclcpp::Publisher<sensor_msgs::msg::CameraInfo>::SharedPtr camera_left_info_;
        rclcpp::Publisher<sensor_msgs::msg::CameraInfo>::SharedPtr camera_right_info_;

        void nerian_info_callback(const nerian_stereo::msg::StereoCameraInfo::SharedPtr msg) const
        {
            camera_left_info_->publish(msg->left_info);
            camera_right_info_->publish(msg->right_info);            
        }
};

int main(int argc, char* argv[]){
    rclcpp::init(argc, argv);
    rclcpp::spin(std::make_shared<CameraInfoSplitter>());
    rclcpp::shutdown();
    return 0;
}