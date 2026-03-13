# Stage 1: Build Flutter Web
FROM debian:latest AS build

RUN apt-get update && apt-get install -y curl git unzip xz-utils libglu1-mesa
RUN git clone https://github.com/flutter/flutter.git /usr/local/flutter
ENV PATH="/usr/local/flutter/bin:/usr/local/flutter/bin/cache/dart-sdk/bin:${PATH}"
RUN flutter doctor
RUN flutter channel stable
RUN flutter upgrade

WORKDIR /app
COPY . .
RUN flutter pub get
RUN flutter build web --release

# Stage 2: Serve with Nginx
FROM nginx:alpine
COPY --from=build /app/build/web /usr/share/nginx/html
# Replace default nginx config to handle proxying /api to backend if needed
# For now, we assume the browser hits 127.0.0.1:5000 directly.
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
