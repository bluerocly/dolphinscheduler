package org.apache.dolphinscheduler.server.master.consumer.message;

public enum MessageTypeEnum {
	unknown_message("0"), dbinsert_message("1"), dbintegrity_message("2");

    private String value;

    MessageTypeEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

    public static MessageTypeEnum getMessageTypeEnum(String value) {
        if (null == value) {
            return MessageTypeEnum.unknown_message;
        }
        MessageTypeEnum[] values = values();
        for (MessageTypeEnum msgType : values) {
            if (value.equalsIgnoreCase(msgType.getValue())) {
                return msgType;
            }
        }
        return MessageTypeEnum.unknown_message;
    }
}
