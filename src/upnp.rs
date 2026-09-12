use std::fmt::Write as _;
use std::sync::OnceLock;

use crate::state::AppState;
use uuid::Uuid;

pub const MEDIA_RENDERER: &str = "urn:schemas-upnp-org:device:MediaRenderer:1";
pub const AV_TRANSPORT: &str = "urn:schemas-upnp-org:service:AVTransport:1";
pub const RENDERING_CONTROL: &str = "urn:schemas-upnp-org:service:RenderingControl:1";
pub const CONNECTION_MANAGER: &str = "urn:schemas-upnp-org:service:ConnectionManager:1";
pub const NIRVANA_SERVICE: &str = "urn:app-bilibili-com:service:NirvanaControl:3";
pub const NIRVANA_DISCOVERY: &str = "urn:schemas-upnp-org:service:NirvanaControl:3";

pub fn description_xml(state: &AppState, nva_port: u16) -> String {
    let base = format!("http://{}:{nva_port}", state.advertise_ip());
    let id = nva_tv_id(state.device_uuid());
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<root xmlns=\"urn:schemas-upnp-org:device-1-0\" xmlns:dlna=\"urn:schemas-dlna-org:device-1-0\">\
<specVersion><major>1</major><minor>0</minor></specVersion><URLBase>{base}</URLBase><device>\
<deviceType>{MEDIA_RENDERER}</deviceType><friendlyName>{name}</friendlyName>\
<manufacturer>Bilibili Inc.</manufacturer><manufacturerURL>https://bilibili.com/</manufacturerURL>\
<modelDescription>云视听小电视</modelDescription><modelName>16s</modelName>\
<modelNumber>1024</modelNumber><modelURL>https://app.bilibili.com/</modelURL>\
<serialNumber>1024</serialNumber><UDN>uuid:{id}</UDN>\
<X_brandName>nva2dlna</X_brandName><hostVersion>25</hostVersion><ottVersion>106400</ottVersion>\
<channelName>master</channelName><capability>254</capability>\
<dlna:X_DLNADOC>DMR-1.50</dlna:X_DLNADOC><dlna:X_DLNACAP>playcontainer-1-0</dlna:X_DLNACAP>\
<serviceList>{services}</serviceList></device></root>",
        name = xml_escape(state.friendly_name()),
        services = service_list(),
    )
}

pub fn nva_tv_id(uuid: Uuid) -> String {
    let compact = uuid.simple().to_string().to_ascii_uppercase();
    format!("XY{compact}{}", &compact[..3])
}

fn service_list() -> String {
    [
        service(
            AV_TRANSPORT,
            "urn:upnp-org:serviceId:AVTransport",
            "/dlna/AVTransport.xml",
            "/AVTransport/action",
            "/AVTransport/event",
        ),
        service(
            RENDERING_CONTROL,
            "urn:upnp-org:serviceId:RenderingControl",
            "/dlna/RenderingControl.xml",
            "/RenderingControl/action",
            "/RenderingControl/event",
        ),
        service(
            CONNECTION_MANAGER,
            "urn:upnp-org:serviceId:ConnectionManager",
            "/dlna/ConnectionManager.xml",
            "/ConnectionManager/action",
            "/ConnectionManager/event",
        ),
        service(
            NIRVANA_SERVICE,
            "urn:app-bilibili-com:serviceId:NirvanaControl",
            "/dlna/NirvanaControl.xml",
            "/NirvanaControl/action",
            "/NirvanaControl/event",
        ),
    ]
    .join("")
}

fn service(kind: &str, id: &str, scpd: &str, control: &str, event: &str) -> String {
    format!(
        "<service><serviceType>{kind}</serviceType><serviceId>{id}</serviceId>\
<controlURL>{control}</controlURL><eventSubURL>{event}</eventSubURL><SCPDURL>{scpd}</SCPDURL></service>"
    )
}

pub fn service_document(path: &str) -> Option<&'static str> {
    let name = path
        .split('?')
        .next()
        .unwrap_or(path)
        .trim_end_matches('/')
        .rsplit('/')
        .next()?
        .to_ascii_lowercase();
    match name.as_str() {
        "nirvanacontrol.xml" => Some(NIRVANA_SCPD.get_or_init(build_nirvana_scpd).as_str()),
        "avtransport.xml" | "renderingcontrol.xml" | "connectionmanager.xml" => Some(EMPTY_SCPD),
        _ => None,
    }
}

pub fn soap_action(header: Option<&str>, body: &str) -> Option<String> {
    if let Some(header) = header {
        let value = header.trim().trim_matches(|ch| ch == '"' || ch == '\'');
        if let Some((_, action)) = value.rsplit_once('#') {
            return valid_action(action).then(|| action.to_owned());
        }
    }
    let body_start = body.find(":Body").or_else(|| body.find("<Body"))?;
    let tail = &body[body_start..];
    let close = tail.find('>')?;
    let child = &tail[close + 1..];
    let start = child.find('<')? + 1;
    let qualified = child[start..]
        .split(|ch: char| ch == '>' || ch.is_ascii_whitespace())
        .next()
        .unwrap_or_default()
        .trim_matches('/');
    let name = qualified.rsplit(':').next().unwrap_or(qualified);
    valid_action(name).then(|| name.to_owned())
}

fn valid_action(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
}

pub fn soap_response(action: &str) -> Result<String, String> {
    let payload = match action {
        "GetAppInfo" => ("<PackageName>com.xiaodianshi.tv.yst</PackageName>\
<AppKey>0000000000000000</AppKey><Signature>0000000000000000</Signature>\
<CurrentSignedIn>1</CurrentSignedIn>")
            .to_owned(),
        "GetAccountInfo" => "<VipInfo>0</VipInfo>".to_owned(),
        "GetPlayInfo" => "<Content>{}</Content>".to_owned(),
        "PrepareForMirrorProjection" => {
            "<ScreenWidth>0</ScreenWidth><ScreenHeight>0</ScreenHeight><PushUrl></PushUrl>"
                .to_owned()
        }
        "SetDanmakuSwitch" | "AppendDanmaku" | "LoginWithCode" | "SwitchQuality" => String::new(),
        _ => return Err(soap_fault(401, "Invalid Action")),
    };
    Ok(format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" \
s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"><s:Body>\
<u:{action}Response xmlns:u=\"{NIRVANA_SERVICE}\">{payload}</u:{action}Response>\
</s:Body></s:Envelope>"
    ))
}

pub fn soap_fault(code: u16, description: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\"><s:Body><s:Fault>\
<faultcode>s:Client</faultcode><faultstring>UPnPError</faultstring><detail>\
<UPnPError xmlns=\"urn:schemas-upnp-org:control-1-0\"><errorCode>{code}</errorCode>\
<errorDescription>{}</errorDescription></UPnPError></detail></s:Fault></s:Body></s:Envelope>",
        xml_escape(description)
    )
}

pub fn xml_escape(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '"' => output.push_str("&quot;"),
            '\'' => output.push_str("&apos;"),
            _ => output.push(ch),
        }
    }
    output
}

pub fn didl(title: &str, media_url: &str, mime: &str) -> String {
    let class = if mime.starts_with("audio/") {
        "object.item.audioItem.musicTrack"
    } else if mime.starts_with("image/") {
        "object.item.imageItem.photo"
    } else {
        "object.item.videoItem"
    };
    let mut xml = String::new();
    write!(
        xml,
        "<DIDL-Lite xmlns=\"urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/\" \
xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:upnp=\"urn:schemas-upnp-org:metadata-1-0/upnp/\">\
<item id=\"0\" parentID=\"0\" restricted=\"1\"><dc:title>{}</dc:title>\
<upnp:class>{class}</upnp:class><res protocolInfo=\"http-get:*:{mime}:*\">{}</res></item></DIDL-Lite>",
        xml_escape(title),
        xml_escape(media_url)
    )
    .expect("writing to String cannot fail");
    xml
}

const EMPTY_SCPD: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<scpd xmlns=\"urn:schemas-upnp-org:service-1-0\"><specVersion><major>1</major><minor>0</minor>\
</specVersion><actionList></actionList><serviceStateTable></serviceStateTable></scpd>";

static NIRVANA_SCPD: OnceLock<String> = OnceLock::new();

fn build_nirvana_scpd() -> String {
    let mut xml = String::with_capacity(4_096);
    xml.push_str(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<scpd xmlns=\"urn:schemas-upnp-org:service-1-0\"><specVersion><major>1</major><minor>0</minor>\
</specVersion><actionList>",
    );
    append_action(
        &mut xml,
        "GetAppInfo",
        &[
            ("PackageName", "out", "A_ARG_TYPE_PackageName"),
            ("AppKey", "out", "A_ARG_TYPE_AppKey"),
            ("Signature", "out", "A_ARG_TYPE_Signature"),
            ("CurrentSignedIn", "out", "SignedIn"),
        ],
    );
    append_action(
        &mut xml,
        "GetAccountInfo",
        &[("VipInfo", "out", "A_ARG_TYPE_UnlimitedInt")],
    );
    append_action(
        &mut xml,
        "GetPlayInfo",
        &[
            ("Params", "in", "A_ARG_TYPE_UnlimitedString"),
            ("Content", "out", "A_ARG_TYPE_UnlimitedString"),
        ],
    );
    append_action(
        &mut xml,
        "PrepareForMirrorProjection",
        &[
            ("ScreenWidth", "out", "A_ARG_TYPE_ScreenResolution"),
            ("ScreenHeight", "out", "A_ARG_TYPE_ScreenResolution"),
            ("PushUrl", "out", "A_ARG_TYPE_UnlimitedString"),
        ],
    );
    append_action(
        &mut xml,
        "SetDanmakuSwitch",
        &[("DesiredSwitch", "in", "DanmakuSwitch")],
    );
    append_action(
        &mut xml,
        "AppendDanmaku",
        &[
            ("Content", "in", "A_ARG_TYPE_UnlimitedString"),
            ("Size", "in", "A_ARG_TYPE_UnlimitedInt"),
            ("Type", "in", "A_ARG_TYPE_UnlimitedInt"),
            ("Color", "in", "A_ARG_TYPE_UnlimitedInt"),
            ("DanmakuId", "in", "A_ARG_TYPE_UnlimitedString"),
            ("Action", "in", "A_ARG_TYPE_UnlimitedString"),
        ],
    );
    append_action(
        &mut xml,
        "LoginWithCode",
        &[("Code", "in", "A_ARG_TYPE_UnlimitedString")],
    );
    append_action(
        &mut xml,
        "SwitchQuality",
        &[("Qn", "in", "A_ARG_TYPE_UnlimitedInt")],
    );
    xml.push_str("</actionList><serviceStateTable>");
    append_state(&mut xml, "A_ARG_TYPE_UnlimitedString", "string", None, &[]);
    append_state(
        &mut xml,
        "A_ARG_TYPE_PackageName",
        "string",
        Some("com.xiaodianshi.tv.yst"),
        &[],
    );
    append_state(
        &mut xml,
        "A_ARG_TYPE_AppKey",
        "string",
        Some("0000000000000000"),
        &[],
    );
    append_state(
        &mut xml,
        "A_ARG_TYPE_Signature",
        "string",
        Some("0000000000000000"),
        &[],
    );
    append_state(&mut xml, "A_ARG_TYPE_UnlimitedInt", "i4", None, &[]);
    append_state(&mut xml, "A_ARG_TYPE_ScreenResolution", "ui4", None, &[]);
    append_state(&mut xml, "SignedIn", "boolean", Some("1"), &["0", "1"]);
    append_state(&mut xml, "DanmakuSwitch", "boolean", Some("1"), &["0", "1"]);
    xml.push_str("</serviceStateTable></scpd>");
    xml
}

fn append_action(xml: &mut String, name: &str, arguments: &[(&str, &str, &str)]) {
    write!(xml, "<action><name>{name}</name>").expect("writing to String cannot fail");
    if !arguments.is_empty() {
        xml.push_str("<argumentList>");
        for (name, direction, related_state) in arguments {
            write!(
                xml,
                "<argument><name>{name}</name><direction>{direction}</direction>\
<relatedStateVariable>{related_state}</relatedStateVariable></argument>"
            )
            .expect("writing to String cannot fail");
        }
        xml.push_str("</argumentList>");
    }
    xml.push_str("</action>");
}

fn append_state(
    xml: &mut String,
    name: &str,
    data_type: &str,
    default_value: Option<&str>,
    allowed_values: &[&str],
) {
    write!(
        xml,
        "<stateVariable sendEvents=\"no\"><name>{name}</name><dataType>{data_type}</dataType>"
    )
    .expect("writing to String cannot fail");
    if let Some(default_value) = default_value {
        write!(xml, "<defaultValue>{default_value}</defaultValue>")
            .expect("writing to String cannot fail");
    }
    if !allowed_values.is_empty() {
        xml.push_str("<allowedValueList>");
        for value in allowed_values {
            write!(xml, "<allowedValue>{value}</allowedValue>")
                .expect("writing to String cannot fail");
        }
        xml.push_str("</allowedValueList>");
    }
    xml.push_str("</stateVariable>");
}

#[cfg(test)]
mod tests {
    use std::{
        net::{Ipv4Addr, SocketAddrV4},
        path::PathBuf,
    };

    use super::*;

    #[test]
    fn escapes_nested_didl_for_soap_in_two_distinct_steps() {
        let didl = didl("A & B", "http://10.0.0.2/a?x=1&y=2", "video/mp2t");
        assert!(didl.contains("A &amp; B"));
        assert!(didl.contains("x=1&amp;y=2"));
        let soap_value = xml_escape(&didl);
        assert!(soap_value.contains("&lt;DIDL-Lite"));
    }

    #[test]
    fn app_info_exposes_only_compatibility_placeholders() {
        let response = soap_response("GetAppInfo").expect("GetAppInfo response");
        assert!(response.contains("<AppKey>0000000000000000</AppKey>"));
        assert!(response.contains("<Signature>0000000000000000</Signature>"));
        assert_eq!(response.matches("0000000000000000").count(), 2);
    }

    #[test]
    fn nva_services_use_the_legacy_child_order() {
        let service = service(
            NIRVANA_SERVICE,
            "urn:app-bilibili-com:serviceId:NirvanaControl",
            "/dlna/NirvanaControl.xml",
            "/NirvanaControl/action",
            "/NirvanaControl/event",
        );
        let control = service.find("<controlURL>").expect("control URL");
        let event = service.find("<eventSubURL>").expect("event URL");
        let scpd = service.find("<SCPDURL>").expect("SCPD URL");
        assert!(control < event && event < scpd);
    }

    #[test]
    fn nva_description_contains_the_captured_tv_model_url() {
        let config = crate::config::RuntimeConfig {
            web_listen: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 8080),
            nva_listen: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 9959),
            advertise_ip: Ipv4Addr::new(192, 0, 2, 10),
            config_path: PathBuf::from("unused-config.json"),
            web_dir: PathBuf::from("web/dist"),
            ffmpeg: PathBuf::from("ffmpeg"),
            friendly_name: "我的小电视".into(),
            device_uuid: Uuid::nil(),
            selected_udn: None,
        };
        let state = AppState::new(&config).expect("test state");
        let description = description_xml(&state, 9959);
        assert!(description.contains("<modelURL>https://app.bilibili.com/</modelURL>"));
        assert!(description.contains("<X_brandName>nva2dlna</X_brandName>"));
        assert!(!description.contains("Meizu"));
    }

    #[test]
    fn nirvana_scpd_declares_arguments_and_related_state_variables() {
        let scpd = service_document("/dlna/NirvanaControl.xml").expect("Nirvana SCPD");
        assert!(scpd.contains("<name>GetAppInfo</name><argumentList>"));
        assert!(scpd.contains("<name>DesiredSwitch</name><direction>in</direction>"));
        assert!(scpd.contains("<relatedStateVariable>DanmakuSwitch</relatedStateVariable>"));
        assert!(scpd.contains("<name>A_ARG_TYPE_AppKey</name><dataType>string</dataType>"));
    }
}
