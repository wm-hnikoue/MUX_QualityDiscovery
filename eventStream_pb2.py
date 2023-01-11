# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: eventStream.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import duration_pb2 as google_dot_protobuf_dot_duration__pb2
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x11\x65ventStream.proto\x12\x0b\x65ventstream\x1a\x1egoogle/protobuf/duration.proto\x1a\x1fgoogle/protobuf/timestamp.proto\"\x84\x18\n\x17\x45xternalVideoViewRecord\x12\x0f\n\x07view_id\x18\x01 \x02(\t\x12\x13\n\x0bproperty_id\x18\x02 \x02(\t\x12.\n\nview_start\x18\x03 \x02(\x0b\x32\x1a.google.protobuf.Timestamp\x12,\n\x08view_end\x18\x04 \x02(\x0b\x32\x1a.google.protobuf.Timestamp\x12\"\n\x06\x65vents\x18\x05 \x03(\x0b\x32\x12.eventstream.Event\x12\x0b\n\x03\x61sn\x18\x06 \x01(\x05\x12\x0f\n\x07\x62rowser\x18\x07 \x01(\t\x12\x17\n\x0f\x62rowser_version\x18\x08 \x01(\t\x12\x0b\n\x03\x63\x64n\x18\t \x01(\t\x12\x0c\n\x04\x63ity\x18\n \x01(\t\x12\x16\n\x0e\x63ontinent_code\x18\x0b \x01(\t\x12\x0f\n\x07\x63ountry\x18\x0c \x01(\t\x12\x14\n\x0c\x63ountry_name\x18\r \x01(\t\x12\x10\n\x08\x63ustom_1\x18\x0e \x01(\t\x12\x10\n\x08\x63ustom_2\x18\x0f \x01(\t\x12\x10\n\x08\x63ustom_3\x18\x10 \x01(\t\x12\x10\n\x08\x63ustom_4\x18\x11 \x01(\t\x12\x10\n\x08\x63ustom_5\x18\x12 \x01(\t\x12\x12\n\nerror_type\x18\x13 \x01(\x05\x12\x1f\n\x17\x65xit_before_video_start\x18\x14 \x01(\x08\x12\x17\n\x0f\x65xperiment_name\x18\x15 \x01(\t\x12\x10\n\x08latitude\x18\x16 \x01(\x01\x12\x11\n\tlongitude\x18\x17 \x01(\x01\x12 \n\x18max_downscale_percentage\x18\x18 \x01(\x02\x12\x1e\n\x16max_upscale_percentage\x18\x19 \x01(\x02\x12\x17\n\x0fmux_api_version\x18\x1a \x01(\t\x12\x19\n\x11mux_embed_version\x18\x1b \x01(\t\x12\x15\n\rmux_viewer_id\x18\x1c \x01(\t\x12\x18\n\x10operating_system\x18\x1d \x01(\t\x12 \n\x18operating_system_version\x18\x1e \x01(\t\x12\x16\n\x0epage_load_time\x18\x1f \x01(\x05\x12\x11\n\tpage_type\x18  \x01(\t\x12\x10\n\x08page_url\x18! \x01(\t\x12\x1e\n\x16playback_success_score\x18\" \x01(\x02\x12\x17\n\x0fplayer_autoplay\x18# \x01(\x08\x12\x19\n\x11player_error_code\x18$ \x01(\t\x12\x1c\n\x14player_error_message\x18% \x01(\t\x12\x15\n\rplayer_height\x18& \x01(\x05\x12\x1a\n\x12player_instance_id\x18\' \x01(\t\x12\x17\n\x0fplayer_language\x18( \x01(\t\x12\x1e\n\x16player_mux_plugin_name\x18) \x01(\t\x12!\n\x19player_mux_plugin_version\x18* \x01(\t\x12\x13\n\x0bplayer_name\x18+ \x01(\t\x12\x15\n\rplayer_poster\x18, \x01(\t\x12\x16\n\x0eplayer_preload\x18- \x01(\x08\x12\x1c\n\x14player_remote_played\x18. \x01(\x08\x12\x17\n\x0fplayer_software\x18/ \x01(\t\x12\x1f\n\x17player_software_version\x18\x30 \x01(\t\x12\x1c\n\x14player_source_domain\x18\x31 \x01(\t\x12\x1e\n\x16player_source_duration\x18\x32 \x01(\x03\x12\x1c\n\x14player_source_height\x18\x33 \x01(\x05\x12\x19\n\x11player_source_url\x18\x34 \x01(\t\x12\x1b\n\x13player_source_width\x18\x35 \x01(\x05\x12\x1b\n\x13player_startup_time\x18\x36 \x01(\x05\x12\x16\n\x0eplayer_version\x18\x37 \x01(\t\x12\x19\n\x11player_view_count\x18\x38 \x01(\x05\x12\x14\n\x0cplayer_width\x18\x39 \x01(\x05\x12\x16\n\x0erebuffer_count\x18: \x01(\x05\x12\x19\n\x11rebuffer_duration\x18; \x01(\x05\x12\x1a\n\x12rebuffer_frequency\x18< \x01(\x02\x12\x1b\n\x13rebuffer_percentage\x18= \x01(\x02\x12\x0e\n\x06region\x18> \x01(\t\x12\x12\n\nsession_id\x18? \x01(\t\x12\x18\n\x10smoothness_score\x18@ \x01(\x02\x12\x17\n\x0fsource_hostname\x18\x41 \x01(\t\x12\x13\n\x0bsource_type\x18\x42 \x01(\t\x12\x1a\n\x12startup_time_score\x18\x43 \x01(\x02\x12\x17\n\x0fsub_property_id\x18\x44 \x01(\t\x12\x17\n\x0fused_fullscreen\x18\x45 \x01(\x08\x12\x1a\n\x12video_content_type\x18\x46 \x01(\t\x12\x16\n\x0evideo_duration\x18G \x01(\x05\x12\x1e\n\x16video_encoding_variant\x18H \x01(\t\x12\x10\n\x08video_id\x18I \x01(\t\x12\x16\n\x0evideo_language\x18J \x01(\t\x12\x16\n\x0evideo_producer\x18K \x01(\t\x12\x1b\n\x13video_quality_score\x18L \x01(\x02\x12\x14\n\x0cvideo_series\x18M \x01(\t\x12\x1a\n\x12video_startup_time\x18N \x01(\x05\x12\x13\n\x0bvideo_title\x18O \x01(\t\x12\x18\n\x10video_variant_id\x18P \x01(\t\x12\x1a\n\x12video_variant_name\x18Q \x01(\t\x12#\n\x1bview_downscaling_percentage\x18R \x01(\x02\x12\"\n\x1aview_max_playhead_position\x18S \x01(\x03\x12\x19\n\x11view_playing_time\x18T \x01(\x03\x12\x17\n\x0fview_seek_count\x18U \x01(\x05\x12\x1a\n\x12view_seek_duration\x18V \x01(\x05\x12\x17\n\x0fview_session_id\x18W \x01(\t\x12(\n view_total_content_playback_time\x18X \x01(\x05\x12\x1e\n\x16view_total_downscaling\x18Y \x01(\x02\x12\x1c\n\x14view_total_upscaling\x18Z \x01(\x02\x12!\n\x19view_upscaling_percentage\x18[ \x01(\x02\x12!\n\x19viewer_application_engine\x18\\ \x01(\t\x12\x1e\n\x16viewer_connection_type\x18] \x01(\t\x12\x1e\n\x16viewer_device_category\x18^ \x01(\t\x12\"\n\x1aviewer_device_manufacturer\x18_ \x01(\t\x12\x1a\n\x12viewer_device_name\x18` \x01(\t\x12\x1f\n\x17viewer_experience_score\x18\x61 \x01(\x02\x12\x1e\n\x16viewer_os_architecture\x18\x62 \x01(\t\x12\x19\n\x11viewer_user_agent\x18\x63 \x01(\t\x12\x16\n\x0eviewer_user_id\x18\x64 \x01(\t\x12\x12\n\nwatch_time\x18\x65 \x01(\x05\x12\x0f\n\x07watched\x18\x66 \x01(\x08\x12 \n\x18weighted_average_bitrate\x18g \x01(\x01\x12!\n\x19preroll_ad_asset_hostname\x18h \x01(\t\x12\x1f\n\x17preroll_ad_tag_hostname\x18i \x01(\t\x12\x16\n\x0epreroll_played\x18j \x01(\x08\x12\x19\n\x11preroll_requested\x18k \x01(\x08\x12\"\n\x1arequests_for_first_preroll\x18l \x01(\x05\x12\'\n\x1fvideo_startup_preroll_load_time\x18m \x01(\x05\x12*\n\"video_startup_preroll_request_time\x18n \x01(\x05\x12\x1b\n\x13max_request_latency\x18o \x01(\x05\x12\x17\n\x0frequest_latency\x18p \x01(\x05\x12\x1a\n\x12request_throughput\x18q \x01(\x05\x12\x13\n\x0bstream_type\x18r \x01(\t\"\xba\x02\n\x05\x45vent\x12\x17\n\x0fsequence_number\x18\x07 \x02(\x04\x12/\n\x0bserver_time\x18\x01 \x02(\x0b\x32\x1a.google.protobuf.Timestamp\x12/\n\x0bviewer_time\x18\x02 \x02(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x30\n\rplayhead_time\x18\x03 \x02(\x0b\x32\x19.google.protobuf.Duration\x12\x0c\n\x04type\x18\x04 \x02(\t\x12\x38\n\x10orientation_info\x18\x05 \x01(\x0b\x32\x1c.eventstream.OrientationInfoH\x00\x12\x34\n\x0erendition_info\x18\x06 \x01(\x0b\x32\x1a.eventstream.RenditionInfoH\x00\x42\x06\n\x04info\"2\n\x0fOrientationInfo\x12\t\n\x01x\x18\x01 \x02(\x01\x12\t\n\x01y\x18\x02 \x02(\x01\x12\t\n\x01z\x18\x03 \x02(\x01\"L\n\rRenditionInfo\x12\r\n\x05width\x18\x01 \x01(\x03\x12\x0e\n\x06height\x18\x02 \x01(\x03\x12\x0b\n\x03\x66ps\x18\x03 \x01(\x01\x12\x0f\n\x07\x62itrate\x18\x04 \x01(\x03')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'eventStream_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _EXTERNALVIDEOVIEWRECORD._serialized_start=100
  _EXTERNALVIDEOVIEWRECORD._serialized_end=3176
  _EVENT._serialized_start=3179
  _EVENT._serialized_end=3493
  _ORIENTATIONINFO._serialized_start=3495
  _ORIENTATIONINFO._serialized_end=3545
  _RENDITIONINFO._serialized_start=3547
  _RENDITIONINFO._serialized_end=3623
# @@protoc_insertion_point(module_scope)
