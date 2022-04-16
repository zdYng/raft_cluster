
if __name__ == '__main__':
    uav_hb_topic = "UAV/Any/RTS/Hb"
    uav_topic = "TT/UAV/Any/RTS/Hb/54"
    if uav_hb_topic in uav_topic:
        uav_id = int(uav_topic.split("/")[-1])
        print()
