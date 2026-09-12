use crate::jobs::{
  YamlAnalysisParams, YamlAnalysisResponse
};

wq::declare_queues!(
  yaml_analysis<YamlAnalysisParams, YamlAnalysisResponse>
);
