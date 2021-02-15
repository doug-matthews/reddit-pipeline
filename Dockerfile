FROM public.ecr.aws/lambda/python:3.8

RUN pip install requests

COPY pushshift_fetch.py   ./
CMD ["pushshift_fetch.handler"]      