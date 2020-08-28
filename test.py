from google.cloud.dataproc_v1.types.shared import Component

optional_components = []
a = [1]

for c in a:
  print(c)
  print(type(c))
  if isinstance(c, int):
    optional_components.append(c)
  if isinstance(c, str):
    optional_components.append(Component[c].value)
  else:
   raise RuntimeError(f'Component type {type(c)} not supported.')

print(optional_components)