# ============================================================================
# ACM certificate (DNS-validated) for the lineage NLB TLS listener. Recommend a
# wildcard cert_domain_name (e.g. *.openlakehousedemos.dev). Gated on
# enable_https (domain_name + hosted_zone_id both set).
# ============================================================================
resource "aws_acm_certificate" "lineage" {
  count             = local.enable_https ? 1 : 0
  domain_name       = var.cert_domain_name != "" ? var.cert_domain_name : var.domain_name
  validation_method = "DNS"
  tags              = var.tags

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_route53_record" "cert_validation" {
  for_each = local.enable_https ? {
    for dvo in aws_acm_certificate.lineage[0].domain_validation_options :
    dvo.domain_name => {
      name   = dvo.resource_record_name
      type   = dvo.resource_record_type
      record = dvo.resource_record_value
    }
  } : {}

  zone_id         = var.hosted_zone_id
  name            = each.value.name
  type            = each.value.type
  records         = [each.value.record]
  ttl             = 60
  allow_overwrite = true
}

resource "aws_acm_certificate_validation" "lineage" {
  count                   = local.enable_https ? 1 : 0
  certificate_arn         = aws_acm_certificate.lineage[0].arn
  validation_record_fqdns = [for r in aws_route53_record.cert_validation : r.fqdn]
}

# ============================================================================
# NLB — public lineage ConnectRPC endpoint.
# An NLB (L4) passes HTTP/2 (h2c) through cleanly, which the Go service speaks
# for ConnectRPC. With HTTPS enabled it terminates TLS on :443 (ALPN
# HTTP2Preferred, so both h2 and HTTP/1.1 clients negotiate) and forwards
# plaintext to the container; without HTTPS it forwards raw TCP on the lineage
# port.
# ============================================================================
resource "aws_lb" "lineage" {
  name               = "${var.name_prefix}-nlb"
  load_balancer_type = "network"
  internal           = false
  subnets            = local.subnet_ids
  tags               = var.tags
}

resource "aws_lb_target_group" "lineage" {
  name        = "${var.name_prefix}-tg"
  port        = var.lineage_port
  protocol    = "TCP"
  target_type = "ip"
  vpc_id      = local.vpc_id
  tags        = var.tags

  health_check {
    protocol            = "TCP"
    interval            = 30
    healthy_threshold   = 3
    unhealthy_threshold = 3
  }
}

resource "aws_lb_listener" "lineage" {
  load_balancer_arn = aws_lb.lineage.arn
  port              = local.enable_https ? 443 : var.lineage_port
  protocol          = local.enable_https ? "TLS" : "TCP"
  ssl_policy        = local.enable_https ? "ELBSecurityPolicy-TLS13-1-2-2021-06" : null
  certificate_arn   = local.enable_https ? aws_acm_certificate_validation.lineage[0].certificate_arn : null
  alpn_policy       = local.enable_https ? "HTTP2Preferred" : null

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.lineage.arn
  }
}

resource "aws_route53_record" "lineage" {
  count   = local.enable_https ? 1 : 0
  zone_id = var.hosted_zone_id
  name    = var.domain_name
  type    = "A"

  alias {
    name                   = aws_lb.lineage.dns_name
    zone_id                = aws_lb.lineage.zone_id
    evaluate_target_health = true
  }
}
