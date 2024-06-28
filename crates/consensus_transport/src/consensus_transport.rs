// This file is @generated by prost-build.
#[derive(serde::Deserialize, serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Dependency {
    #[prost(bytes = "vec", tag = "1")]
    pub timestamp: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub timestamp_zero: ::prost::alloc::vec::Vec<u8>,
}
#[derive(serde::Deserialize, serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreAcceptRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub event: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub timestamp_zero: ::prost::alloc::vec::Vec<u8>,
}
#[derive(serde::Deserialize, serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreAcceptResponse {
    #[prost(bytes = "vec", tag = "1")]
    pub timestamp: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, repeated, tag = "2")]
    pub dependencies: ::prost::alloc::vec::Vec<Dependency>,
}
#[derive(serde::Deserialize, serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AcceptRequest {
    #[prost(uint32, tag = "1")]
    pub ballot: u32,
    #[prost(bytes = "vec", tag = "2")]
    pub event: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub timestamp_zero: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    pub timestamp: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, repeated, tag = "5")]
    pub dependencies: ::prost::alloc::vec::Vec<Dependency>,
}
#[derive(serde::Deserialize, serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AcceptResponse {
    #[prost(message, repeated, tag = "1")]
    pub dependencies: ::prost::alloc::vec::Vec<Dependency>,
    #[prost(bool, tag = "2")]
    pub nack: bool,
}
#[derive(serde::Deserialize, serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommitRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub event: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub timestamp_zero: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub timestamp: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, repeated, tag = "4")]
    pub dependencies: ::prost::alloc::vec::Vec<Dependency>,
}
/// repeated Dependency results = 1; // Theoretically not needed?
#[derive(serde::Deserialize, serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommitResponse {}
#[derive(serde::Deserialize, serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplyRequest {
    #[prost(bytes = "vec", tag = "1")]
    pub event: ::prost::alloc::vec::Vec<u8>,
    /// This is extra to make it easier to identify the event
    #[prost(bytes = "vec", tag = "2")]
    pub timestamp_zero: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub timestamp: ::prost::alloc::vec::Vec<u8>,
    /// bytes result = 5; // Theoretically not needed?
    #[prost(message, repeated, tag = "4")]
    pub dependencies: ::prost::alloc::vec::Vec<Dependency>,
}
#[derive(serde::Deserialize, serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplyResponse {}
#[derive(serde::Deserialize, serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RecoverRequest {
    #[prost(uint32, tag = "1")]
    pub ballot: u32,
    #[prost(bytes = "vec", tag = "2")]
    pub event: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub timestamp_zero: ::prost::alloc::vec::Vec<u8>,
}
#[derive(serde::Deserialize, serde::Serialize)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RecoverResponse {
    #[prost(enumeration = "State", tag = "1")]
    pub local_state: i32,
    #[prost(message, repeated, tag = "2")]
    pub wait: ::prost::alloc::vec::Vec<Dependency>,
    #[prost(message, repeated, tag = "3")]
    pub superseding: ::prost::alloc::vec::Vec<Dependency>,
    #[prost(bytes = "vec", tag = "4")]
    pub timestamp: ::prost::alloc::vec::Vec<u8>,
    #[prost(bool, tag = "5")]
    pub nack: bool,
}
#[derive(
    serde::Deserialize,
    serde::Serialize,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    ::prost::Enumeration,
)]
#[repr(i32)]
pub enum State {
    Undefined = 0,
    PreAccepted = 1,
    Accepted = 2,
    Commited = 3,
    Applied = 4,
    Recover = 5,
}
impl State {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            State::Undefined => "STATE_UNDEFINED",
            State::PreAccepted => "STATE_PRE_ACCEPTED",
            State::Accepted => "STATE_ACCEPTED",
            State::Commited => "STATE_COMMITED",
            State::Applied => "STATE_APPLIED",
            State::Recover => "STATE_RECOVER",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "STATE_UNDEFINED" => Some(Self::Undefined),
            "STATE_PRE_ACCEPTED" => Some(Self::PreAccepted),
            "STATE_ACCEPTED" => Some(Self::Accepted),
            "STATE_COMMITED" => Some(Self::Commited),
            "STATE_APPLIED" => Some(Self::Applied),
            "STATE_RECOVER" => Some(Self::Recover),
            _ => None,
        }
    }
}
/// Generated client implementations.
pub mod consensus_transport_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::http::Uri;
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct ConsensusTransportClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ConsensusTransportClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ConsensusTransportClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> ConsensusTransportClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            ConsensusTransportClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        pub async fn pre_accept(
            &mut self,
            request: impl tonic::IntoRequest<super::PreAcceptRequest>,
        ) -> std::result::Result<tonic::Response<super::PreAcceptResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/consensus_transport.ConsensusTransport/PreAccept",
            );
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new(
                "consensus_transport.ConsensusTransport",
                "PreAccept",
            ));
            self.inner.unary(req, path, codec).await
        }
        pub async fn commit(
            &mut self,
            request: impl tonic::IntoRequest<super::CommitRequest>,
        ) -> std::result::Result<tonic::Response<super::CommitResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/consensus_transport.ConsensusTransport/Commit",
            );
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new(
                "consensus_transport.ConsensusTransport",
                "Commit",
            ));
            self.inner.unary(req, path, codec).await
        }
        pub async fn accept(
            &mut self,
            request: impl tonic::IntoRequest<super::AcceptRequest>,
        ) -> std::result::Result<tonic::Response<super::AcceptResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/consensus_transport.ConsensusTransport/Accept",
            );
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new(
                "consensus_transport.ConsensusTransport",
                "Accept",
            ));
            self.inner.unary(req, path, codec).await
        }
        pub async fn apply(
            &mut self,
            request: impl tonic::IntoRequest<super::ApplyRequest>,
        ) -> std::result::Result<tonic::Response<super::ApplyResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/consensus_transport.ConsensusTransport/Apply",
            );
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new(
                "consensus_transport.ConsensusTransport",
                "Apply",
            ));
            self.inner.unary(req, path, codec).await
        }
        pub async fn recover(
            &mut self,
            request: impl tonic::IntoRequest<super::RecoverRequest>,
        ) -> std::result::Result<tonic::Response<super::RecoverResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/consensus_transport.ConsensusTransport/Recover",
            );
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new(
                "consensus_transport.ConsensusTransport",
                "Recover",
            ));
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod consensus_transport_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with ConsensusTransportServer.
    #[async_trait]
    pub trait ConsensusTransport: Send + Sync + 'static {
        async fn pre_accept(
            &self,
            request: tonic::Request<super::PreAcceptRequest>,
        ) -> std::result::Result<tonic::Response<super::PreAcceptResponse>, tonic::Status>;
        async fn commit(
            &self,
            request: tonic::Request<super::CommitRequest>,
        ) -> std::result::Result<tonic::Response<super::CommitResponse>, tonic::Status>;
        async fn accept(
            &self,
            request: tonic::Request<super::AcceptRequest>,
        ) -> std::result::Result<tonic::Response<super::AcceptResponse>, tonic::Status>;
        async fn apply(
            &self,
            request: tonic::Request<super::ApplyRequest>,
        ) -> std::result::Result<tonic::Response<super::ApplyResponse>, tonic::Status>;
        async fn recover(
            &self,
            request: tonic::Request<super::RecoverRequest>,
        ) -> std::result::Result<tonic::Response<super::RecoverResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct ConsensusTransportServer<T: ConsensusTransport> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: ConsensusTransport> ConsensusTransportServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for ConsensusTransportServer<T>
    where
        T: ConsensusTransport,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/consensus_transport.ConsensusTransport/PreAccept" => {
                    #[allow(non_camel_case_types)]
                    struct PreAcceptSvc<T: ConsensusTransport>(pub Arc<T>);
                    impl<T: ConsensusTransport> tonic::server::UnaryService<super::PreAcceptRequest>
                        for PreAcceptSvc<T>
                    {
                        type Response = super::PreAcceptResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PreAcceptRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as ConsensusTransport>::pre_accept(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = PreAcceptSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/consensus_transport.ConsensusTransport/Commit" => {
                    #[allow(non_camel_case_types)]
                    struct CommitSvc<T: ConsensusTransport>(pub Arc<T>);
                    impl<T: ConsensusTransport> tonic::server::UnaryService<super::CommitRequest> for CommitSvc<T> {
                        type Response = super::CommitResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CommitRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as ConsensusTransport>::commit(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CommitSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/consensus_transport.ConsensusTransport/Accept" => {
                    #[allow(non_camel_case_types)]
                    struct AcceptSvc<T: ConsensusTransport>(pub Arc<T>);
                    impl<T: ConsensusTransport> tonic::server::UnaryService<super::AcceptRequest> for AcceptSvc<T> {
                        type Response = super::AcceptResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AcceptRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as ConsensusTransport>::accept(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = AcceptSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/consensus_transport.ConsensusTransport/Apply" => {
                    #[allow(non_camel_case_types)]
                    struct ApplySvc<T: ConsensusTransport>(pub Arc<T>);
                    impl<T: ConsensusTransport> tonic::server::UnaryService<super::ApplyRequest> for ApplySvc<T> {
                        type Response = super::ApplyResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ApplyRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as ConsensusTransport>::apply(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = ApplySvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/consensus_transport.ConsensusTransport/Recover" => {
                    #[allow(non_camel_case_types)]
                    struct RecoverSvc<T: ConsensusTransport>(pub Arc<T>);
                    impl<T: ConsensusTransport> tonic::server::UnaryService<super::RecoverRequest> for RecoverSvc<T> {
                        type Response = super::RecoverResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RecoverRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as ConsensusTransport>::recover(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = RecoverSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(empty_body())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: ConsensusTransport> Clone for ConsensusTransportServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: ConsensusTransport> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: ConsensusTransport> tonic::server::NamedService for ConsensusTransportServer<T> {
        const NAME: &'static str = "consensus_transport.ConsensusTransport";
    }
}
