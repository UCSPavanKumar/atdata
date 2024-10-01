import yaml
import os
def handle_save_config(event):
  tokens   = os.environ['config_output'].split('/')
  bucket   = tokens[-8] 
  s3 = boto3.resource('s3')
  data = {'config_dir':event('input_dir')}
  yaml_output = yaml.dump(data, default_flow_style=False)
  object = s3.Object(bucket, 'config.yaml')
  object.put(Body=yaml_output)
  
